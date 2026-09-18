#region Using directives
using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Configuration;
using System.Globalization;
using System.IO;
using System.Text;
using System.Text.Json;
using gnaDataClasses;
using GNATrackGeometryDataCapture;
using T4Dlibrary;
using Exporter = GNATrackGeometryExport.GNATrackGeometryExport;
#endregion

namespace TrackGeometryReport
{
    internal sealed class DBTrackGeometryRun : IDisposable
    {
        #region Constants and state
        internal const string TriggerFileName = "DBTRackGeometryTrigger.txt";
        internal const string TimestampFormat = "yyyy-MM-dd HH:mm:ss";
        private const string AnchorKey = "DBWriteScheduleAnchorLocal";
        private const string IntervalKey = "IntervalBetweenDBwrite";
        private const string BlockSizeKey = "DBBlockSizeHrs";
        private const int StateVersion = 1;
        private readonly FileStream runLock;
        private readonly string projectTitle;
        private readonly string workbookPath;
        private readonly string statePath;
        private bool completed;
        private bool disposed;

        public bool IsDatabaseWrite { get; private set; }
        public bool ScheduledReportDue { get; private set; }
        public DateTime? TriggerUtc { get; private set; }
        public DateTime? BlockStartUtc { get; private set; }
        public TimeZoneInfo ProjectTimeZone { get; }
        public NameValueCollection Settings { get; }
        public string Decision { get; private set; } = string.Empty;

        private DBTrackGeometryRun(FileStream runLock, string projectTitle, string workbookPath,
            string statePath, TimeZoneInfo projectTimeZone, NameValueCollection settings)
        {
            this.runLock = runLock;
            this.projectTitle = projectTitle;
            this.workbookPath = workbookPath;
            this.statePath = statePath;
            ProjectTimeZone = projectTimeZone;
            Settings = new NameValueCollection(col: settings);
            Settings["WriteDataToDBTrackGeometry"] = "No";
        }
        #endregion

        #region Start-of-run decision and temporary settings
        public static DBTrackGeometryRun BeginRun(NameValueCollection settings, string projectTitle,
            string masterWorkbookPath, string systemLogsFolder, string timeZoneId,
            DateTime nowUtc, Func<bool> scheduledReportDue)
        {
            NameValueCollection validatedSettings = settings
                ?? throw new ArgumentNullException(paramName: nameof(settings));
            Func<bool> scheduleCheck = scheduledReportDue
                ?? throw new ArgumentNullException(paramName: nameof(scheduledReportDue));
            if (nowUtc.Kind != DateTimeKind.Utc)
                throw new ArgumentException(message: "The run time must be UTC.", paramName: nameof(nowUtc));
            string fullWorkbookPath = Path.GetFullPath(path: masterWorkbookPath);
            string fullLogsPath = Path.GetFullPath(path: systemLogsFolder);
            Directory.CreateDirectory(path: fullLogsPath);
            string statePath = Path.Combine(path1: fullLogsPath, path2: TriggerFileName);
            FileStream lease;
            try
            {
                // Normal reports and database runs share this lease for the whole invocation.
                // The sidecar remains present; exclusivity belongs to the open handle.
                lease = new FileStream(path: statePath + ".lock", mode: FileMode.OpenOrCreate,
                    access: FileAccess.ReadWrite, share: FileShare.None);
            }
            catch (IOException ex)
            {
                throw new IOException(message:
                    "Cannot acquire the TrackGeometryReport run lock. Another invocation may still be running. " +
                    "No calculation or database write was started.", innerException: ex);
            }

            DBTrackGeometryRun? run = null;
            try
            {
                run = new DBTrackGeometryRun(runLock: lease, projectTitle: projectTitle,
                    workbookPath: fullWorkbookPath, statePath: statePath,
                    projectTimeZone: TimeZoneInfo.FindSystemTimeZoneById(id: timeZoneId), settings: validatedSettings);
                run.ScheduledReportDue = scheduleCheck();
                if (run.ScheduledReportDue)
                {
                    run.Decision = "Scheduled reporting takes precedence; database trigger remains pending.";
                    return run;
                }
                if (!ReadYesNo(settings: validatedSettings, key: "WriteDataToDBTrackGeometry"))
                {
                    run.Decision = "Timed DBTrackGeometry writing is disabled.";
                    return run;
                }

                TimeSpan interval = ReadHours(settings: validatedSettings, key: IntervalKey);
                TimeSpan blockSize = ReadHours(settings: validatedSettings, key: BlockSizeKey);
                string anchorText = Required(settings: validatedSettings, key: AnchorKey);
                if (!DateTime.TryParseExact(s: anchorText,
                    formats: new[] { TimestampFormat, "yyyy-MM-dd HH:mm" },
                    provider: CultureInfo.InvariantCulture, style: DateTimeStyles.None, result: out DateTime anchorLocal))
                    throw new ConfigurationErrorsException(message: $"{AnchorKey} must use yyyy-MM-dd HH:mm:ss or yyyy-MM-dd HH:mm.");
                anchorLocal = DateTime.SpecifyKind(value: anchorLocal, kind: DateTimeKind.Unspecified);
                if (run.ProjectTimeZone.IsInvalidTime(dateTime: anchorLocal) || run.ProjectTimeZone.IsAmbiguousTime(dateTime: anchorLocal))
                    throw new ConfigurationErrorsException(message:
                        $"{AnchorKey} is invalid or ambiguous at a daylight-saving transition. Choose an unambiguous local time.");
                DateTime anchorUtc = TimeZoneInfo.ConvertTimeToUtc(dateTime: anchorLocal, sourceTimeZone: run.ProjectTimeZone);
                DateTime? previousUtc = run.ReadLastSuccessfulTrigger();
                DateTime? trigger = SelectLatestDueTrigger(anchorUtc: anchorUtc, interval: interval,
                    previousSuccessfulUtc: previousUtc, nowUtc: nowUtc);
                if (!trigger.HasValue)
                {
                    run.Decision = "No new DBTrackGeometry trigger is due.";
                    return run;
                }
                run.TriggerUtc = trigger;
                run.BlockStartUtc = trigger.Value.Subtract(value: blockSize);
                run.IsDatabaseWrite = true;
                foreach (string key in new[] { "prepareReferenceData", "debug", "stopAtAlarmMessage", "SendEmails",
                    "AlarmVersion", "recordHistoricData", "LatestValueOnly", "IssueDailyAlarmStatusSummary" })
                    run.Settings[key] = "No";
                run.Settings["WriteDataToDBTrackGeometry"] = "Yes";
                run.Settings["TimeBlockType"] = "Manual";
                run.Settings["BlockSizeHrs"] = Required(settings: validatedSettings, key: BlockSizeKey);
                run.Settings["manualBlockStart"] = TimeZoneInfo.ConvertTimeFromUtc(
                    dateTime: run.BlockStartUtc.Value, destinationTimeZone: run.ProjectTimeZone)
                    .ToString(format: TimestampFormat, provider: CultureInfo.InvariantCulture);
                run.Settings["manualBlockEnd"] = TimeZoneInfo.ConvertTimeFromUtc(
                    dateTime: trigger.Value, destinationTimeZone: run.ProjectTimeZone)
                    .ToString(format: TimestampFormat, provider: CultureInfo.InvariantCulture);
                run.Decision = $"Database mode: {run.BlockStartUtc:O} to {run.TriggerUtc:O}. " +
                    "Earlier missed triggers are skipped. Completion is recorded only after a successful write.";
                return run;
            }
            catch
            {
                if (run is null) lease.Dispose();
                else run.Dispose();
                throw;
            }
        }

        internal static DateTime? SelectLatestDueTrigger(DateTime anchorUtc, TimeSpan interval,
            DateTime? previousSuccessfulUtc, DateTime nowUtc)
        {
            if (anchorUtc.Kind != DateTimeKind.Utc || nowUtc.Kind != DateTimeKind.Utc ||
                (previousSuccessfulUtc.HasValue && previousSuccessfulUtc.Value.Kind != DateTimeKind.Utc))
                throw new ArgumentException(message: "Schedule timestamps must be UTC.");
            if (interval.Ticks <= 0 || interval.Ticks % TimeSpan.TicksPerSecond != 0)
                throw new ArgumentOutOfRangeException(paramName: nameof(interval), message: "Interval must be positive whole seconds.");
            if (previousSuccessfulUtc > nowUtc)
                throw new InvalidDataException(message: "The persisted database trigger is in the future. Check the system clock and trigger file.");
            if (nowUtc < anchorUtc) return null;
            long elapsedIntervals = (nowUtc.Ticks - anchorUtc.Ticks) / interval.Ticks;
            DateTime latest = anchorUtc.AddTicks(value: checked(elapsedIntervals * interval.Ticks));
            return previousSuccessfulUtc.HasValue && latest <= previousSuccessfulUtc.Value ? null : latest;
        }

        internal static bool ShouldWriteHistoricData(bool recordHistoricData, bool scheduledEmailRequired,
            bool emailRequired, bool databaseWriteMode)
        {
            return !databaseWriteMode && emailRequired && (scheduledEmailRequired || recordHistoricData);
        }
        #endregion

        #region End-of-calculation database write
        public void WriteToDatabase(Exporter exporter, DBTrackGeometryExportContext exportContext,
            T4Dapi t4dapi, RuntimeEnvironment runtimeEnvironment, List<Points> prismList,
            List<TrackGeometryPair> trackPairList, bool trackGeometryCalculationSucceeded)
        {
            if (!trackGeometryCalculationSucceeded)
                throw new InvalidOperationException(message:
                    "Slew/versine calculation failed. The database trigger has not been completed.");
            CompleteAfterSuccessfulWrite(writeEpochs: () =>
            {
                DBTrackGeometryDataCapture capture = new();
                DBTrackGeometryCaptureResult captured = capture.Capture(prismList: prismList, trackPairList: trackPairList,
                    strReportUtc: TriggerUtc!.Value.ToString(format: TimestampFormat, provider: CultureInfo.InvariantCulture),
                    latestReadingTimes: t4dapi.ReadReferenceLatestReadingTimes(runtimeEnvironment: runtimeEnvironment));
                DBTrackGeometryEpochBatch batch = exporter.PrepareEpochBatchAsync(exportContext: exportContext,
                    pointEpochs: captured.PointEpochs, pairEpochs: captured.PairEpochs).GetAwaiter().GetResult();
                Exporter.EchoEpochBatchValidationSummary(epochBatch: batch);
                DBTrackGeometryEpochWriteResult result = exporter.WriteEpochBatchAsync(
                    exportContext: exportContext, epochBatch: batch).GetAwaiter().GetResult();
                Exporter.EchoEpochWriteSummary(writeResult: result);
                if (!string.Equals(a: result.Outcome, b: "Success", comparisonType: StringComparison.Ordinal))
                    throw new InvalidOperationException(message:
                        $"DBTrackGeometry outcome was {result.Outcome}; trigger completion was not recorded.");
            });
        }

        internal void CompleteAfterSuccessfulWrite(Action writeEpochs)
        {
            if (disposed) throw new ObjectDisposedException(objectName: nameof(DBTrackGeometryRun));
            if (!IsDatabaseWrite || !TriggerUtc.HasValue || completed)
                throw new InvalidOperationException(message: "There is no pending database write in this run.");
            Action write = writeEpochs ?? throw new ArgumentNullException(paramName: nameof(writeEpochs));
            write();
            PersistSuccessfulTrigger();
            completed = true;
        }
        #endregion

        #region Trigger state persistence and cleanup
        private DateTime? ReadLastSuccessfulTrigger()
        {
            if (!File.Exists(path: statePath)) return null;
            TriggerState state = JsonSerializer.Deserialize<TriggerState>(json: File.ReadAllText(path: statePath))
                ?? throw new InvalidDataException(message: "DBTrackGeometry trigger file is empty or invalid.");
            if (state.Version != StateVersion ||
                !string.Equals(a: state.ProjectTitle, b: projectTitle, comparisonType: StringComparison.OrdinalIgnoreCase) ||
                !string.Equals(a: state.MasterWorkbookPath, b: workbookPath, comparisonType: StringComparison.OrdinalIgnoreCase) ||
                state.LastSuccessfulTriggerUtc.Kind != DateTimeKind.Utc ||
                state.LastSuccessfulTriggerUtc == DateTime.MinValue ||
                state.LastSuccessfulTriggerUtc.Ticks % TimeSpan.TicksPerSecond != 0)
                throw new InvalidDataException(message:
                    "DBTrackGeometry trigger file has an invalid timestamp, version or project/workbook identity. " +
                    "Use a separate SystemLogsFolder for each project/workbook; the file was not changed.");
            return state.LastSuccessfulTriggerUtc;
        }

        private void PersistSuccessfulTrigger()
        {
            string temporaryPath = statePath + "." + Guid.NewGuid().ToString(format: "N") + ".tmp";
            try
            {
                TriggerState state = new()
                {
                    Version = StateVersion, ProjectTitle = projectTitle, MasterWorkbookPath = workbookPath,
                    LastSuccessfulTriggerUtc = TriggerUtc!.Value
                };
                byte[] bytes = Encoding.UTF8.GetBytes(s: JsonSerializer.Serialize(value: state,
                    options: new JsonSerializerOptions { WriteIndented = true }));
                using (FileStream output = new(path: temporaryPath, mode: FileMode.CreateNew,
                    access: FileAccess.Write, share: FileShare.None))
                {
                    output.Write(buffer: bytes, offset: 0, count: bytes.Length);
                    output.Flush(flushToDisk: true);
                }
                // Rename within the same directory while the exclusive run lease is held.
                // Unlike File.Replace, this does not require restoring the destination ACL.
                File.Move(sourceFileName: temporaryPath, destFileName: statePath, overwrite: true);
            }
            finally
            {
                if (File.Exists(path: temporaryPath)) File.Delete(path: temporaryPath);
            }
        }

        private sealed class TriggerState
        {
            public int Version { get; set; }
            public string ProjectTitle { get; set; } = string.Empty;
            public string MasterWorkbookPath { get; set; } = string.Empty;
            public DateTime LastSuccessfulTriggerUtc { get; set; }
        }

        public void Dispose()
        {
            if (disposed) return;
            runLock.Dispose();
            disposed = true;
        }
        #endregion

        #region Configuration parsing
        private static string Required(NameValueCollection settings, string key)
        {
            string value = settings[key]?.Trim()
                ?? throw new ConfigurationErrorsException(message: $"Missing appSetting '{key}'.");
            if (value.Length == 0) throw new ConfigurationErrorsException(message: $"Empty appSetting '{key}'.");
            return value;
        }

        private static bool ReadYesNo(NameValueCollection settings, string key)
        {
            string value = Required(settings: settings, key: key);
            if (value.Equals(value: "Yes", comparisonType: StringComparison.OrdinalIgnoreCase)) return true;
            if (value.Equals(value: "No", comparisonType: StringComparison.OrdinalIgnoreCase)) return false;
            throw new ConfigurationErrorsException(message: $"'{key}' must be Yes or No.");
        }

        private static TimeSpan ReadHours(NameValueCollection settings, string key)
        {
            if (!decimal.TryParse(s: Required(settings: settings, key: key), style: NumberStyles.AllowDecimalPoint,
                provider: CultureInfo.InvariantCulture, result: out decimal hours) || hours <= 0 ||
                hours > (decimal)TimeSpan.MaxValue.Ticks / TimeSpan.TicksPerHour)
                throw new ConfigurationErrorsException(message: $"'{key}' must be a positive number of hours.");
            decimal seconds = hours * 3600m;
            if (seconds != decimal.Truncate(d: seconds))
                throw new ConfigurationErrorsException(message: $"'{key}' must resolve to whole seconds.");
            return TimeSpan.FromTicks(value: checked((long)(hours * TimeSpan.TicksPerHour)));
        }
        #endregion
    }
}
