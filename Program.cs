using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Configuration;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;

using databaseAPI;

using GNA_CommercialLicenseValidator;

using gnaDataClasses;

using GNAgeneraltools;

using GNAspreadsheettools;

using GNAsurveytools;

using T4Dlibrary;

using static System.Runtime.InteropServices.JavaScript.JSType;

//using System.ComponentModel;
//using System.Data;
//using System.Data.Common;
//using System.Diagnostics;
//using System.IO;
//using System.Linq;
//using System.Reflection;
//using System.Reflection.Metadata.Ecma335;
//using EASendMail;
//using Microsoft.Data.SqlClient;
//using OfficeOpenXml;
//using Twilio.Rest.Api.V2010.Account;
//using Twilio.Rest.Sync.V1.Service.SyncStream;
//using Twilio.TwiML.Messaging;
//using Twilio.TwiML.Voice;
//using static T4Dlibrary.T4Dapi;

namespace TrackGeometryReport
{
    class Program
    {
        #region Constants

        private const string EmailTransmissionSuccess =
            "Email sent successfully";

        private const string SystemActivityLogFileName =
            "SystemActivityLog.txt";

        private const string TwilioCredentialsFileName =
            "TwilioCredentials.bin";

        #endregion

        static void Main()
        {
            string strFatalCrashLogFullPath = Path.Combine(
                path1: AppContext.BaseDirectory,
                path2: "fatal_crash.log");
            // This is a generic and expanded version of the SPN010 track geometry reports
            // additional featureds are added to make it more user friendly.
            // 20260412



            try
            {


#pragma warning disable CS0162
#pragma warning disable CS8600
#pragma warning disable CS8601
#pragma warning disable CS8602
#pragma warning disable CS8604




                //================[Instantiate the classes]======================================

                #region Setting state
                Console.OutputEncoding = System.Text.Encoding.Unicode;
                gnaTools gnaT = new();
                GNAsurveycalcs gnaSurvey = new();
                dbAPI gnaDBAPI = new();
                spreadsheetAPI gnaSpreadsheetAPI = new(db: gnaDBAPI);
                T4Dapi t4dapi = new();

                string strTab1 = "     ";
                string strTab2 = "        ";


                Console.OutputEncoding = System.Text.Encoding.Unicode;
                Console.Out.Flush();
                Console.Clear();

                #endregion

                #region Header
                gnaT.WelcomeMessage($"TrackGeometryReport {BuildInfo.BuildDateString()}");
                #endregion

                #region Config validation
                int headingNo = 1;
                Console.WriteLine($"{headingNo++}. System Check");
                gnaT.VerifyLocalConfig();
                Console.WriteLine($"{strTab1}VerifyLocalConfig returned OK");
                #endregion

                #region Read config early
                NameValueCollection config = ConfigurationManager.AppSettings;
                bool freezeScreen = ConfigParsing.GetBoolYesNo(config, "freezeScreen");
                bool prepareReferenceData = ConfigParsing.GetBoolYesNo(config, "prepareReferenceData");
                bool computeMean = ConfigParsing.GetBoolYesNo(config, "computeMean");
                bool debug = ConfigParsing.GetBoolYesNo(config, "debug");
                string strcomputeMeans = computeMean ? "Yes" : "No";
                string strFreezeScreen = freezeScreen ? "Yes" : "No";
                string strPrepareReferenceData = prepareReferenceData ? "Yes" : "No";
                #endregion

                #region License validation

                Console.WriteLine($"{strTab1}Validating software licenses");
                string licenseCode = config["LicenseCode"] ?? string.Empty;
                if (licenseCode.Length == 0)
                    throw new ConfigurationErrorsException("\nLicenseCode missing/empty.");
                LicenseValidator.ValidateLicense("GNATGR", licenseCode);
                Console.WriteLine($"{strTab2}Software validated");

                // Set the T4DAPI license: Software license expires 20261201
                string T4DAPI_licenseCode = "Dm4eGwoTaGxraGpr";

                string Result = t4dapi.SetCommercial(T4DAPI_licenseCode);
                if (Result != "YES")
                {
                    Console.WriteLine($"{strTab2}\nT4DAPI license validation failed: {Result}");
                    throw new Exception("T4DAPI license validation failed.");
                }
                else
                {
                    Console.WriteLine($"{strTab2}T4DAPI validated");
                }
                gnaT.epplusLicense();
                Console.WriteLine($"{strTab1}Done");
                #endregion

                #region EPPlus license
                string strEpplusLicenseContext = ConfigParsing.GetRequiredString(config, "EPPlus:ExcelPackage.LicenseContext");
                gnaT.epplusLicense();
                #endregion

                #region Workbook variables
                // Always required
                string strExcelPath = ConfigParsing.GetRequiredString(config, "ExcelPath");
                string strExcelFile = ConfigParsing.GetRequiredString(config, "ExcelFile");
                string strReferenceWorksheet = ConfigParsing.GetRequiredString(config, "ReferenceWorksheet");

                // Optional (may be absent from config AND from this executable's usage)
                string? strSurveyWorksheet = config["SurveyWorksheet"]?.Trim();
                string? strTrackGeometryWorksheet = config["TrackGeometryWorksheet"]?.Trim();
                string? strCalibrationWorksheet = config["CalibrationWorksheet"]?.Trim();
                string? strAlarmsWorksheet = config["AlarmsWorksheet"]?.Trim();
                string? strCopingWorksheet = config["CopingWorksheet"]?.Trim();
                string? strHistoricDhWorksheet = config["HistoricDhWorksheet"]?.Trim();
                string? strHistoricTopWorksheet = config["HistoricTopWorksheet"]?.Trim();
                string? strHistoricdHWorksheet = config["HistoricdHWorksheet"]?.Trim();
                string? strHistoricTwistWorksheet = config["HistoricTwistWorksheet"]?.Trim();
                string? strHistoricLongTwistWorksheet = config["HistoricLongTwistWorksheet"]?.Trim();
                string? strHistoricCantWorksheet = config["HistoricCantWorksheet"]?.Trim();
                string? strHistoricCopingWorksheet = config["HistoricCopingWorksheet"]?.Trim();
                string? strHistoricSlewWorksheet = config["HistoricSlewWorksheet"]?.Trim();

                string? strReportSpec = config["ReportSpec"]?.Trim();
                string? strWorkbookPassword = config["WorkbookPassword"]?.Trim();

                #endregion

                #region Check whether workbook is open

                string strMasterWorkbookFullPath = Path.Combine(strExcelPath, strExcelFile);


                if (gnaSpreadsheetAPI.IsWorkbookOpen(
                    strWorkbookFullPath: strMasterWorkbookFullPath))
                {
                    string message =
                        $"\n{strTab1}The Excel workbook is currently open or locked:\n " +
                        $"'{strMasterWorkbookFullPath}'.";
                    Console.WriteLine($"{message}\nExecution stopped...\n");
                    Environment.Exit(exitCode: 0);
                }
                else
                {
                    Console.WriteLine($"{strTab1}{strMasterWorkbookFullPath} ready");
                }
                #endregion

                #region Config variables
                Console.WriteLine($"{headingNo++}. System variables");

                string strDBconnection = ConfigurationManager.ConnectionStrings["DBconnectionString"].ConnectionString;

                string strClient = ConfigParsing.GetRequiredString(config, "Client");

                string strProjectTitle = ConfigParsing.GetRequiredString(config, "ProjectTitle");

                int iFirstDataRow = ConfigParsing.GetRequiredInt(config, "FirstDataRow");
                int iFirstDataCol = ConfigParsing.GetRequiredInt(config, "FirstDataCol");
                int iFirstOutputRow = ConfigParsing.GetRequiredInt(config, "FirstOutputRow");
                int iFirstTrackRow = ConfigParsing.GetRequiredInt(config, "FirstTrackRow");

                string strTimeBlockType = ConfigParsing.GetRequiredString(config, "TimeBlockType");
                string strManualBlockStart = ConfigParsing.GetRequiredString(config, "manualBlockStart");
                string strManualBlockEnd = ConfigParsing.GetRequiredString(config, "manualBlockEnd");
                string strBlockSizeHrs = ConfigParsing.GetRequiredString(config, "BlockSizeHrs");

                string strStopAtAlarmMessage = config["stopAtAlarmMessage"];

                bool alarmVersion = ConfigParsing.GetBoolYesNo(
                    appSettings: config,
                    key: "AlarmVersion");

                bool recordHistoricData = ConfigParsing.GetBoolYesNo(
                    appSettings: config,
                    key: "recordHistoricData");

                string strAlarmVersion = alarmVersion ? "Yes" : "No";
                string strRecordHistoricData = recordHistoricData ? "Yes" : "No";

                string strDeleteMissingValues = config["DeleteMissingValues"];
                string strLatestValueOnly = config["LatestValueOnly"];
                #endregion

                #region Report variables
                string strContractTitle = ConfigParsing.GetRequiredString(config, "ContractTitle");
                string strReportType = ConfigParsing.GetRequiredString(config, "ReportType");
                string strIncludeHistoricTwist = config["includeHistoricTwist"];
                string strIncludeHistoricSettlement = config["includeHistoricSettlement"];
                string strIncludeHistoricTop = config["includeHistoricTop"];
                string strIncludeMissingTargets = config["includeMissingTargets"];
                #endregion

                #region System variables
                Console.WriteLine($"{strTab1}Done");
                #endregion

                #region General variables

                Console.WriteLine($"{strTab1}General variables");

                string strReferenceLineTerminalsEaNaEbNb = CleanConfig(config["ReferenceLineTerminalsEaNaEbNb"]);

                string strComputeMeanDeltas = CleanConfig(config["computeMean"]);
                if (strComputeMeanDeltas.Length == 0) strComputeMeanDeltas = "No";

                string strUpdateSensorList = CleanConfig(config["updateSensorList"]);
                if (strUpdateSensorList.Length == 0) strUpdateSensorList = "No";

                string strIncludeCoping = CleanConfig(config["includeCoping"]);
                if (strIncludeCoping.Length == 0) strIncludeCoping = "No";

                string strIssueDailyAlarmStatusSummary = CleanConfig(config["IssueDailyAlarmStatusSummary"]);
                if (strIssueDailyAlarmStatusSummary.Length == 0) strIssueDailyAlarmStatusSummary = "No";

                string strSystemLogsFolder = CleanConfig(config["SystemLogsFolder"]);
                if (strSystemLogsFolder.Length == 0) strSystemLogsFolder = @"C:\__SystemLogs\";

                string strSystemAlarmfolder = CleanConfig(config["SystemAlarmFolder"]);
                if (strSystemAlarmfolder.Length == 0) strSystemAlarmfolder = @"C:\__SystemAlarms\";

                string strSystemCredentialsFolder = CleanConfig(config["SystemCredentialsFolder"]);
                if (strSystemCredentialsFolder.Length == 0) strSystemCredentialsFolder = @"C:\__SystemCredentials\";

                Directory.CreateDirectory(path: strSystemLogsFolder);
                Directory.CreateDirectory(path: strSystemAlarmfolder);

                strFatalCrashLogFullPath = Path.Combine(
                    path1: strSystemLogsFolder,
                    path2: "fatal_crash.log");

                var cs = ConfigurationManager.ConnectionStrings["DBconnectionString"];
                if (cs == null || string.IsNullOrWhiteSpace(cs.ConnectionString))
                {
                    string message = "\nMissing connection string 'DBconnectionString'.";
                    Console.WriteLine(message);
                    throw new ConfigurationErrorsException(message);
                }

                string strFirstDataRow = iFirstDataRow.ToString(CultureInfo.InvariantCulture);
                string strFirstOutputRow = iFirstOutputRow.ToString(CultureInfo.InvariantCulture);

                string strExcelWorkbookFullPath = Path.Combine(strExcelPath, strExcelFile);
                if (!File.Exists(strExcelWorkbookFullPath))
                {
                    string message = $"Excel workbook not found: '{strExcelWorkbookFullPath}'.";
                    Console.WriteLine(message);
                    throw new FileNotFoundException(message, strExcelWorkbookFullPath);
                }

                //string? strSPN010alarms = config["AlarmNotifications"];

                double dblTimeZoneOffset = gnaDBAPI.getProjectTimeZoneOffset(strDBconnection, strProjectTitle);

                #endregion

                #region Time Zone Details

                string strTimeZoneID =
                    ConfigParsing.GetRequiredString(
                        appSettings: config,
                        key: "TimeZoneId");

                _ = TimeZoneInfo.FindSystemTimeZoneById(
                    id: strTimeZoneID);

                #endregion

                #region Email settings
                Console.WriteLine($"{strTab1}Email settings");

                string strSendEmail = CleanConfig(config["SendEmails"]);
                string strIsBodyHtml = CleanConfig(config["IsBodyHtml"]);
                string strEmailTransmissionDays = CleanConfig(config["EmailTransmissionDays"]);
                string strEmailTransmissionTime = CleanConfig(config["EmailTransmissionTime"]);

                string strEmailLogin = CleanConfig(config["EmailLogin"]);
                string strEmailPassword = CleanConfig(config["EmailPassword"]);
                string strEmailFrom = CleanConfig(config["EmailFrom"]);
                string strEmailRecipients = CleanConfig(config["EmailRecipients"]);
                dblTimeZoneOffset = gnaDBAPI.getProjectTimeZoneOffset(strDBconnection, strProjectTitle);

                EmailCredentials emailCreds = gnaT.BuildEmailCredentials(
                    strEmailLogin: strEmailLogin,
                    strEmailPassword: strEmailPassword,
                    strEmailFrom: strEmailFrom,
                    strEmailRecipients: strEmailRecipients,
                    strSendEmail: strSendEmail,
                    strIsBodyHtml: strIsBodyHtml,
                    strEmailTransmissionDays: strEmailTransmissionDays,
                    strEmailTransmissionTime: strEmailTransmissionTime,
                    dblTimeZoneOffset: dblTimeZoneOffset,
                    strSystemLogsFolder: strSystemLogsFolder,
                    strTimeZoneId: strTimeZoneID);

                #endregion

                #region Email transmission settings
                bool blnShouldSend = gnaT.ShouldTransmitEmail(
                    emailCredentials: emailCreds);

                if (blnShouldSend)
                {
                    Console.WriteLine(
                        $"{strTab2}Scheduled email will be sent.");
                }
                else
                {
                    Console.WriteLine(
                        $"{strTab2}Scheduled email is not due. " +
                        $"Alarm notifications remain independently enabled.");
                }
                #endregion

                #region SMS settings
                Console.WriteLine($"{strTab1}SMS settings");

                string strSMSTitle = ConfigParsing.GetRequiredString(
                    appSettings: config,
                    key: "SMSTitle");

                List<(
                    int Index,
                    string ConfigurationKey,
                    string PhoneNumber,
                    SmsNotificationGroup NotificationGroup)>
                    configuredSmsRecipients = new();

                string[] smsConfigurationKeys =
                    config.AllKeys ?? Array.Empty<string>();

                for (int iKeyIndex = 0;
                    iKeyIndex < smsConfigurationKeys.Length;
                    iKeyIndex++)
                {
                    string strConfigurationKey =
                        smsConfigurationKeys[iKeyIndex] ?? string.Empty;

                    string strRequiredPrefix;
                    SmsNotificationGroup notificationGroup;

                    if (strConfigurationKey.StartsWith(
                        value: "RecipientPhone",
                        comparisonType:
                            StringComparison.OrdinalIgnoreCase))
                    {
                        strRequiredPrefix = "RecipientPhone";
                        notificationGroup =
                            SmsNotificationGroup.All;
                    }
                    else if (strConfigurationKey.StartsWith(
                        value: "RedPhone",
                        comparisonType:
                            StringComparison.OrdinalIgnoreCase))
                    {
                        strRequiredPrefix = "RedPhone";
                        notificationGroup =
                            SmsNotificationGroup.Red;
                    }
                    else
                    {
                        continue;
                    }

                    string strNumericSuffix = strConfigurationKey
                        .Substring(
                            startIndex: strRequiredPrefix.Length)
                        .Trim();

                    bool blnSuffixIsValid = int.TryParse(
                        s: strNumericSuffix,
                        style: NumberStyles.None,
                        provider: CultureInfo.InvariantCulture,
                        result: out int iRecipientIndex) &&
                        iRecipientIndex > 0;

                    if (!blnSuffixIsValid)
                    {
                        throw new ConfigurationErrorsException(
                            message:
                                $"Invalid SMS recipient key " +
                                $"'{strConfigurationKey}'. Expected " +
                                $"'{strRequiredPrefix}' followed by a " +
                                "positive integer.");
                    }

                    string strPhoneNumber = gnaT.NormalizePhoneNumber(
                        rawValue: config[strConfigurationKey],
                        keyName: strConfigurationKey);

                    configuredSmsRecipients.Add(
                        item:
                            (
                                Index: iRecipientIndex,
                                ConfigurationKey: strConfigurationKey,
                                PhoneNumber: strPhoneNumber,
                                NotificationGroup: notificationGroup));
                }

                configuredSmsRecipients = configuredSmsRecipients
                    .OrderBy(
                        keySelector: recipient =>
                            recipient.NotificationGroup)
                    .ThenBy(
                        keySelector: recipient =>
                            recipient.Index)
                    .ThenBy(
                        keySelector: recipient =>
                            recipient.ConfigurationKey,
                        comparer: StringComparer.OrdinalIgnoreCase)
                    .ToList();

                Dictionary<string, string> configurationKeyByPhoneNumber =
                    new(comparer: StringComparer.Ordinal);

                List<SmsRecipient> smsRecipients = new();

                for (int iRecipientIndex = 0;
                    iRecipientIndex < configuredSmsRecipients.Count;
                    iRecipientIndex++)
                {
                    var configuredRecipient =
                        configuredSmsRecipients[iRecipientIndex];

                    if (configurationKeyByPhoneNumber.TryGetValue(
                        key: configuredRecipient.PhoneNumber,
                        value: out string? strExistingConfigurationKey))
                    {
                        throw new ConfigurationErrorsException(
                            message:
                                $"SMS telephone number " +
                                $"'{configuredRecipient.PhoneNumber}' is " +
                                $"configured more than once: " +
                                $"'{strExistingConfigurationKey}' and " +
                                $"'{configuredRecipient.ConfigurationKey}'.");
                    }

                    configurationKeyByPhoneNumber.Add(
                        key: configuredRecipient.PhoneNumber,
                        value: configuredRecipient.ConfigurationKey);

                    smsRecipients.Add(
                        item:
                            new SmsRecipient(
                                configurationKey:
                                    configuredRecipient.ConfigurationKey,
                                phoneNumber:
                                    configuredRecipient.PhoneNumber,
                                notificationGroup:
                                    configuredRecipient.NotificationGroup));
                }
                #endregion

                #region Operational Variables

                // ---- Arrays and collections ----
                string[] strRO1 = new string[50];
                string[] strWorksheetName = new string[50];
                string[,] strSensorID = new string[5000, 2];
                string[,] strPointDeltas = new string[5000, 2];
                string[] strPointNames;

                // ---- Configuration-derived values ----
                string strCoordinateOrder = config["CoordinateOrder"];
                string strSendEmails = config["SendEmails"];

                // ---- Time block and messaging ----
                string strTimeBlockStartLocal = "";
                string strTimeBlockEndLocal = "";
                string strTimeBlockStartUTC = "";
                string strTimeBlockEndUTC = "";
                string strEmailTime = "";
                string strDateTime = "";


                // ---- Working strings and file paths ----
                string strMasterFile = "";
                string strWorkingFile = "";
                string strExportFile = "";

                // ---- Row and column positions ----
                int iRow = Convert.ToInt32(strFirstDataRow);
                int iReferenceFirstDataRow = Convert.ToInt32(strFirstDataRow);


                Console.WriteLine($"{strTab1}Assigned");
                #endregion

                #region Alarm and Historic Data Configuration

                // AlarmVersion and recordHistoricData are independent configuration
                // controls. Neither value is modified by the email schedule.

                bool blnWriteHistoricData =
                    t4dapi.ShouldWriteHistoricData(
                        alarmVersion: alarmVersion,
                        blnShouldSend: blnShouldSend,
                        strTimeBlockType: strTimeBlockType);

                if (strRecordHistoricData == "No")
                {
                    blnWriteHistoricData = false;
                }

                if ((strRecordHistoricData == "No") && (strTimeBlockType == "Manual"))
                {
                    blnWriteHistoricData = false;
                }

                Console.WriteLine(
                    $"{strTab1}AlarmVersion: {strAlarmVersion}");
                Console.WriteLine(
                    $"{strTab1}  Time Block: {strTimeBlockType}");
                Console.WriteLine(
                    $"{strTab1}Record historic data: {strRecordHistoricData}");

                #endregion

                #region Populate the RuntimeEnvironment class

                gnaDataClasses.RuntimeEnvironment runtimeEnvironment = new()
                {
                    // ---- Database ----
                    DbConnectionString = strDBconnection,
                    ProjectTitle = strProjectTitle,
                    ReportType = strReportType,

                    // ---- Time Zone ----
                    TimeZoneID = strTimeZoneID,

                    // --- System folders ---
                    SystemLogsFolder = strSystemLogsFolder,
                    SystemAlarmFolder = strSystemAlarmfolder,
                    SystemCredentialsFolder = strSystemCredentialsFolder,

                    // ---- Workbook ----
                    ExcelPath = strExcelPath,
                    ExcelFile = strExcelFile,

                    // ---- Permissions ----
                    RecordHistoricData = strRecordHistoricData,

                    // ---- Worksheets ----
                    ReferenceWorksheet = strReferenceWorksheet,
                    SurveyWorksheet = strSurveyWorksheet,
                    CopingWorksheet = strCopingWorksheet,
                    TrackGeometryWorksheet = strTrackGeometryWorksheet,
                    HistoricCantWorksheet = strHistoricCantWorksheet,
                    HistoricTopWorksheet = strHistoricTopWorksheet,
                    HistoricdHWorksheet = strHistoricdHWorksheet,
                    HistoricTwistWorksheet = strHistoricTwistWorksheet,
                    HistoricLongTwistWorksheet = strHistoricLongTwistWorksheet,
                    HistoricCopingWorksheet = strHistoricCopingWorksheet,
                    HistoricSlewWorksheet = strHistoricSlewWorksheet,

                    // ---- Row/Col configuration ----
                    FirstDataRow = iFirstDataRow,
                    FirstDataCol = iFirstDataCol,
                    FirstOutputRow = iFirstOutputRow,
                    FirstTrackRow = iFirstTrackRow
                };

                #endregion

                #region Clean exit
                void FinishAndExit(string strReportType)
                {
                    Console.WriteLine($"\n{strReportType} report completed...\n\n");
                    gnaT.freezeScreen(strFreezeScreen);
                }
                #endregion

                #region Environment check
                Console.WriteLine($"{headingNo++}. Check system environment");
                if (strFreezeScreen == "Yes")
                {
                    Console.WriteLine($"{strTab1}Check DB connection");
                    gnaDBAPI.testDBconnection(strDBconnection);
                    Console.WriteLine($"{strTab2}Done");

                    Console.WriteLine($"{strTab1}Check existence of workbook & Worksheets");

                    Console.WriteLine($"{strTab2}Project: {strProjectTitle}");
                    Console.WriteLine($"{strTab2}Report type: {strReportSpec}");
                    Console.WriteLine($"{strTab2}Master workbook: {strExcelFile}");

                    string strResult = t4dapi.GetProjectID(strDBconnection, strProjectTitle);
                    Console.WriteLine($"{strTab2}ProjectID: {strResult}");

                    gnaSpreadsheetAPI.checkWorksheetExists(strMasterWorkbookFullPath, strReferenceWorksheet);
                    gnaSpreadsheetAPI.checkWorksheetExists(strMasterWorkbookFullPath, strSurveyWorksheet);
                    gnaSpreadsheetAPI.checkWorksheetExists(strMasterWorkbookFullPath, strTrackGeometryWorksheet);
                    gnaSpreadsheetAPI.checkWorksheetExists(strMasterWorkbookFullPath, strAlarmsWorksheet);

                    if (blnWriteHistoricData)
                    {
                        gnaSpreadsheetAPI.checkWorksheetExists(strMasterWorkbookFullPath, strHistoricCantWorksheet);
                        gnaSpreadsheetAPI.checkWorksheetExists(strMasterWorkbookFullPath, strHistoricTwistWorksheet);
                        gnaSpreadsheetAPI.checkWorksheetExists(strMasterWorkbookFullPath, strHistoricLongTwistWorksheet);
                        gnaSpreadsheetAPI.checkWorksheetExists(strMasterWorkbookFullPath, strHistoricTwistWorksheet);
                        gnaSpreadsheetAPI.checkWorksheetExists(strMasterWorkbookFullPath, strHistoricLongTwistWorksheet);
                        gnaSpreadsheetAPI.checkWorksheetExists(strMasterWorkbookFullPath, strHistoricTopWorksheet);
                        gnaSpreadsheetAPI.checkWorksheetExists(strMasterWorkbookFullPath, strHistoricdHWorksheet);
                        gnaSpreadsheetAPI.checkWorksheetExists(strMasterWorkbookFullPath, strHistoricSlewWorksheet);
                    }

                    if (strIncludeCoping.Equals(
                        value: "Yes",
                        comparisonType: StringComparison.OrdinalIgnoreCase))
                    {
                        gnaSpreadsheetAPI.checkWorksheetExists(strMasterWorkbookFullPath, strCopingWorksheet);
                        gnaSpreadsheetAPI.checkWorksheetExists(strMasterWorkbookFullPath, strHistoricCopingWorksheet);
                    }

                    Console.WriteLine($"{strTab1}Done");
                }
                else
                {
                    Console.WriteLine($"{strTab1}Environment check skipped");
                }



                #endregion

                #region Populate the Track Element list
                Console.WriteLine($"{headingNo++}. Populate Track Elements List");
                List<TrackElements> trackElementsList = t4dapi.populateTrackElements(env: runtimeEnvironment);
                Console.WriteLine($"{strTab1}Done");
                #endregion

                #region Time blocks
                Console.WriteLine($"{headingNo++}. Time blocks");
                List<Tuple<string, string>> subBlocks = new();

                dblTimeZoneOffset = gnaDBAPI.getProjectTimeZoneOffset(strDBconnection, strProjectTitle);
                Console.WriteLine($"{strTab1}Project time zone offset: {dblTimeZoneOffset} hrs");


                switch (strTimeBlockType)
                {
                    case "Historic":
                        subBlocks = gnaT.prepareTimeBlocksWithTimeZoneOffset(
                            strTimeBlockType: "Historic",
                            strBlockSizeHrs: strBlockSizeHrs,
                            strManualBlockStart: strManualBlockStart,
                            strManualBlockEnd: strManualBlockEnd,
                            dblTimeZoneOffset: dblTimeZoneOffset);
                        break;

                    case "Manual":
                        subBlocks = gnaT.prepareTimeBlocksWithTimeZoneOffset(
                            strTimeBlockType: "Manual",
                            strManualBlockStart: strManualBlockStart,
                            strManualBlockEnd: strManualBlockEnd,
                            dblTimeZoneOffset: dblTimeZoneOffset);
                        break;

                    case "Schedule":
                        subBlocks = gnaT.prepareTimeBlocksWithTimeZoneOffset(
                            strTimeBlockType: "Schedule",
                            strBlockSizeHrs: strBlockSizeHrs,
                            dblTimeZoneOffset: dblTimeZoneOffset);
                        break;

                    default:
                        throw new ArgumentException(
                            message:
                                $"Unknown TimeBlockType " +
                                $"'{strTimeBlockType}'. Expected Manual, " +
                                "Schedule, or Historic.",
                            paramName: nameof(strTimeBlockType));
                }

                string strTimeStampLocal;
                if (strTimeBlockType == "Manual")
                {
                    string strTemp = strEmailTime.Replace(":", "").Replace("-", "").Replace(" ", "_");
                    strExportFile = strExcelPath + strContractTitle + "_" + strReportType + "_" + strTemp + ".xlsx";
                    strWorkingFile = strExportFile;
                    strMasterFile = strMasterWorkbookFullPath;
                    strTimeStampLocal = strTemp;
                }
                else
                {
                    strExportFile = strExcelPath + strContractTitle + "_" + strReportType + "_" + "DateTime" + ".xlsx";
                    strWorkingFile = strExportFile;
                    strMasterFile = strMasterWorkbookFullPath;
                    strTimeStampLocal = strDateTime;
                }

                Console.WriteLine($"{strTab1}Done");
                #endregion

                #region Survey Worksheet update
                Console.WriteLine($"{headingNo++}. {strSurveyWorksheet} worksheet update");

                if (prepareReferenceData)
                {
                    Console.WriteLine($"{strTab1}Read point names");
                    strPointNames = gnaSpreadsheetAPI.readPointNames(
                        strMasterWorkbookFullPath,
                        strSurveyWorksheet,
                        iFirstDataRow.ToString(System.Globalization.CultureInfo.InvariantCulture));

                    Console.WriteLine($"{strTab1}Extract SensorID");
                    strSensorID = gnaDBAPI.getSensorIDfromDB(strDBconnection, strPointNames, strProjectTitle);

                    if (debug)
                    {
                        int counter = 0;
                        Console.WriteLine($"\nstrProjectTitle: {strProjectTitle}");

                        while (counter < strSensorID.GetLength(0))
                        {
                            string name = (strSensorID[counter, 0] ?? string.Empty).Trim();
                            if (name == "NoMore") break;
                            string id = (strSensorID[counter, 1] ?? string.Empty).Trim();
                            Console.WriteLine($"{counter}  {name}  {id}");
                            counter++;
                        }
                        Console.WriteLine("\n");
                    }

                    Console.WriteLine($"{strTab1}Update SensorID");
                    gnaSpreadsheetAPI.writeSensorID(
                        strMasterWorkbookFullPath,
                        strSurveyWorksheet,
                        strSensorID,
                        iFirstDataRow.ToString(System.Globalization.CultureInfo.InvariantCulture));
                    Console.WriteLine($"{strTab1}Done");
                }
                else
                {
                    Console.WriteLine($"{strTab1}No {strSurveyWorksheet} preparation ");
                }
                #endregion

                #region Create prism sensor list
                Console.WriteLine($"{headingNo++}. Create sensor list: Prisms");
                List<Points> prismList = gnaSpreadsheetAPI.GetPrismConstantData(RuntimeEnvironment: runtimeEnvironment);
                Console.WriteLine($"{strTab1}Done");
                #endregion

                #region Run header log
                Console.WriteLine($"{headingNo++}. Write header log");
                {
                    string runHeader =
                        $"GNA_TrackGeometryReport | Run start | Build={BuildInfo.BuildDateString()} | Project='{strProjectTitle}' | Contract='{strContractTitle}' | " +
                        $"Mode={(prepareReferenceData ? "prepareReferenceData" : "Export")} | TimeBlockType='{strTimeBlockType}' | " +
                        $"ManualStart='{strManualBlockStart}' | ManualEnd='{strManualBlockEnd}' | BlockSizeHrs='{strBlockSizeHrs}' | " +
                        $"computeMean='{strcomputeMeans}' | " + $"include Coping='{strIncludeCoping}' | " +
                        $"Workbook='{strExcelWorkbookFullPath}' | SurveyWS='{strSurveyWorksheet}' | FirstRow={iFirstDataRow}";
                    gnaT.updateSystemLogFile(strSystemLogsFolder, runHeader);
                }
                Console.WriteLine($"{strTab1}Done");
                #endregion

                #region Prepare reference data
                Console.WriteLine($"{headingNo++}. Prepare reference data");

                List<SensorObservation> blockResults = new();

                if (prepareReferenceData)
                {
                    Console.WriteLine($"{strTab1}Extract prism reference data");

                    string blockStartUTC = gnaT.convertLocalToUTCWithTimeZoneOffset(
                        localTime: strManualBlockStart,
                        dblTimeZoneOffset: dblTimeZoneOffset);
                    string blockEndUTC = gnaT.convertLocalToUTCWithTimeZoneOffset(
                        localTime: strManualBlockEnd,
                        dblTimeZoneOffset: dblTimeZoneOffset);

                    List<Points> referenceDeltas = t4dapi.GetAllPointsMeanDeltas(
                        dbConnection: strDBconnection,
                        projectTitle: strProjectTitle,
                        timeBlockStartUTC: blockStartUTC,
                        timeBlockEndUTC: blockEndUTC,
                        iTimeIntervalHours: null);


                    #region Echo selected Points fields to screen if no deltas were retrieved
                    if (referenceDeltas.Count == 0)
                    {
                        Console.WriteLine("\nNo deltas were returned..");
                        Console.WriteLine($"prismList: {prismList.Count}");
                        Console.WriteLine($"strTimeBlockType: {strTimeBlockType}");
                        Console.WriteLine($"blockStartUTC: {blockStartUTC}");
                        Console.WriteLine($"blockEndUTC: {blockEndUTC}");
                        Console.WriteLine($"strComputeMeanDeltas: {strComputeMeanDeltas}");
                        Console.WriteLine($"dblTimeZoneOffset: {dblTimeZoneOffset}\n");
                    }

                    // pass the extracted values across into the parent prismList

                    prismList = t4dapi.combinePointsLists(parentList: prismList, childList: referenceDeltas);

                    prismList = t4dapi.removeOutliers(
                        pointsList: prismList,
                        checkDistance: 0.3);

                    string result = t4dapi.writeDeltasToReferenceWorksheet(
                        prismList: prismList,
                        blockStartUTC: blockStartUTC,
                        blockEndUTC: blockEndUTC,
                        runtimeEnvironment: runtimeEnvironment);
                    Console.WriteLine($"{strTab1}{result}");







                    Console.WriteLine($"\n{strReferenceWorksheet} worksheet updated with reference values.");
                    goto ThatsAllFolks;

                    #endregion

                }
                else
                {
                    Console.WriteLine($"{strTab1}Reference data preparation skipped");
                }
                #endregion

                #region Time block processing
                Console.WriteLine($"{headingNo++}. Process time blocks: {strTimeBlockType}");
                if (!prepareReferenceData)
                {

                    #region Initial settings and variables
                    strDateTime = DateTime.Now.ToString("yyyyMMdd_HHmm");
                    string strDateTimeUTC = DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss");   //2022-07-26 13:45:15
                    string strTimeStamp = "";
                    string strReportTime = "";
                    Console.WriteLine($"{headingNo++}. Block Processing");
                    Console.WriteLine($"{strTab1}Timeblock Type: {strTimeBlockType}");
                    #endregion

                    #region Preparing export file name suffix
                    string timeBlockSuffix = strTimeBlockType switch
                    {
                        "Manual" => "m",
                        "Historic" => "h",
                        "Schedule" => "",
                        _ => throw new ArgumentException(
                            message: $"Unknown TimeBlockType '{strTimeBlockType}'. Expected Manual, Historic, or Schedule.",
                            paramName: nameof(strTimeBlockType))
                    };
                    #endregion


    
                    if ((strRecordHistoricData == "No") && (strTimeBlockType == "Manual"))
                    {
                        blnWriteHistoricData = false;
                    }

                    foreach (var block in subBlocks)
                    {

                        #region Preparing time block strings
                        // Note that these time blocks have been adjusted for time zone offset
                        strTimeBlockStartUTC = block.Item1;
                        strTimeBlockEndUTC = block.Item2;
                        strTimeBlockStartLocal = gnaT.convertUTCToLocalWithTimeZoneOffset(strTimeBlockStartUTC, dblTimeZoneOffset).Trim();
                        strTimeBlockEndLocal = gnaT.convertUTCToLocalWithTimeZoneOffset(strTimeBlockEndUTC, dblTimeZoneOffset).Trim();
                        string strBlockStart = gnaT.NormalizeTimeStampToString(strTimeBlockStartUTC);
                        string strBlockEnd = gnaT.NormalizeTimeStampToString(strTimeBlockEndUTC);
                        strTimeStamp = strTimeBlockEndLocal + "\n(local)";
                        strReportTime = t4dapi.prepareReportTime(
                            strTimeBlockEndLocal);
                        Console.WriteLine($"{strTab2}{strTimeBlockStartLocal} (local)");
                        Console.WriteLine($"{strTab2}{strTimeBlockEndLocal} (local)");
                        #endregion

                        #region Populate prism time block values
                        foreach (Points prism in prismList)
                        {
                            prism.TimeBlockStartUTC = strTimeBlockStartUTC;
                            prism.TimeBlockEndUTC = strTimeBlockEndUTC;
                            prism.UTCtime = strTimeBlockEndUTC;
                        }
                        #endregion

                        #region Build export file name
                        strExportFile = Path.Combine(
                            path1: strExcelPath,
                            path2:
                                $"{strContractTitle}_{strReportType}_" +
                                $"{strReportTime}{timeBlockSuffix}.xlsx");
                        #endregion

                        #region Read Deltas
                        // read deltas from the db for the current time block
                        List<Points> currentDeltas = t4dapi.GetAllPointsMeanDeltas(
                            dbConnection: strDBconnection,
                            projectTitle: strProjectTitle,
                            timeBlockStartUTC: strTimeBlockStartUTC,
                            timeBlockEndUTC: strTimeBlockEndUTC,
                            iTimeIntervalHours: null);
                        #endregion

                        #region Remove Outliers
                        currentDeltas = t4dapi.removeOutliers(
                            pointsList: currentDeltas,
                            checkDistance: 100);

                        prismList = t4dapi.combinePointsLists(parentList: prismList, childList: currentDeltas);

                        prismList = t4dapi.removeOutliers(
                            pointsList: prismList,
                            checkDistance: 100);

                        prismList = t4dapi.checkForMissingReadings(
                            pointList: prismList);
                        #endregion

                        #region Writing geometry to workbook

                        // verified prismList is correct

                        string result = t4dapi.writeDeltasToReferenceWorksheet(
                            prismList: prismList,
                            blockStartUTC: strTimeBlockStartUTC,
                            blockEndUTC: strTimeBlockEndUTC,
                            runtimeEnvironment: runtimeEnvironment);

                        Console.WriteLine($"{strTab1}{result}");
                        if (!string.Equals(result, "writeDeltasToReferenceWorksheet: Success.", StringComparison.Ordinal))
                        {
                            Console.WriteLine($"\nExecution halted: {result}");
                            throw new InvalidOperationException(message: result);
                        }

                        // the reference worksheet is now populated
                        prismList = t4dapi.CalculatePrismCoordinatesAndTopOfRail(
                            prismList: prismList);


                        // verified prismList is correct

                        List<TrackGeometryPair> trackPairList = t4dapi.extractTrackPair(
                            env: runtimeEnvironment,
                            prismList: prismList);

                        // verified prismList is correct

                        trackPairList = t4dapi.computeTrackGeometry(
                            trackPairList: trackPairList,
                            prismList: prismList);

                        // verified prismList is correct

                        result = t4dapi.writeTrackGeometryToWorkbook(
                            env: runtimeEnvironment,
                            trackPairList: trackPairList,
                            prismList: prismList,
                            strTimeBlockStartUTC: strTimeBlockStartUTC,
                            strTimeBlockEndUTC: strTimeBlockEndUTC,
                            alarmVersion: alarmVersion,
                            blnShouldSend: blnShouldSend,
                            strTimeBlockType: strTimeBlockType,
                            strRecordHistoricData: strRecordHistoricData);

                        #endregion

                        #region Compute slew

                        Console.WriteLine(
                            $"{strTab1}Compute track slew");

                        bool blnTrackSlewSuccess =
                            gnaSpreadsheetAPI.computeTrackSlew(
                                env: runtimeEnvironment,
                                prismList: prismList,
                                trackPairList: trackPairList,
                                strTimeBlockEndUTC: strTimeBlockEndUTC);

                        Console.WriteLine(
                            $"{strTab2}Success: {blnTrackSlewSuccess}");

                        bool blnWriteHistoricSlew =
                            t4dapi.ShouldWriteHistoricData(
                                alarmVersion: alarmVersion,
                                blnShouldSend: blnShouldSend,
                                strTimeBlockType: strTimeBlockType);

                        if(strTimeBlockType== "Manual" && strRecordHistoricData == "No")
                        {
                            blnWriteHistoricSlew = false;
                            blnWriteHistoricData = false;
                        }


                        if (blnWriteHistoricSlew)
                        {
                            #region Historic Slew Worksheet Header

                            string strHeaderTimeFormatted = DateTime.ParseExact(
                                s: strTimeBlockEndUTC,
                                format: "yyyy-MM-dd HH:mm:ss",
                                provider: CultureInfo.InvariantCulture)
                            .ToString(
                                format: "yyyy-MM-dd\nHH'h'mm",
                                provider: CultureInfo.InvariantCulture);

                            string strHistoricSlewHeaderTitle =
                                "Slew\n(mm)\n" +
                                strHeaderTimeFormatted;

                            #endregion

                            #region Write Historic Slew

                            string strHistoricSlewResult =
                                t4dapi.WriteHistoricTrackPairGeometrySeries(
                                    env: runtimeEnvironment,
                                    trackPairList: trackPairList,
                                    worksheetName:
                                        runtimeEnvironment.HistoricSlewWorksheet,
                                    headerTitle: strHistoricSlewHeaderTitle,
                                    valueSelector: pair =>
                                        pair.Slew.HasValue
                                            ? pair.Slew.Value * 1000.0
                                            : null);

                            Console.WriteLine(
                                $"{strTab2}{strHistoricSlewResult}\n" +
                                $"{strTab2}Done");

                            #endregion
                        }

                        #endregion

                        #region Coping displacement

                        Console.WriteLine(
                            $"{strTab1}Coping displacement");


                        if (strTimeBlockType == "Manual" && strRecordHistoricData == "No")
                        {
                            blnWriteHistoricData = false;
                        }


                        if (strIncludeCoping.Equals(
                            value: "Yes",
                            comparisonType: StringComparison.OrdinalIgnoreCase))
                        {
                            gnaSpreadsheetAPI.copingDisplacement(
                                runtimeEnvironment: runtimeEnvironment,
                                strReferenceLineTerminalsEaNaEbNb:
                                    strReferenceLineTerminalsEaNaEbNb,
                                strTimeBlockEndUTC: strTimeBlockEndUTC,
                                blnWriteHistoricData: blnWriteHistoricData );

                            Console.WriteLine(
                                $"{strTab2}Done");
                        }
                        else
                        {
                            Console.WriteLine(
                                $"{strTab2}No coping");
                        }

                        #endregion

                        #region Check Alarm State

                        Console.WriteLine(
                            $"{headingNo++}. Checking alarm state");

                        string strAlarmMessage =
                            gnaSpreadsheetAPI.SPN010AlarmState(
                                strMasterFile,
                                strAlarmsWorksheet,
                                iFirstTrackRow,
                                strIncludeMissingTargets);

                        AlarmEvaluationResult alarmEvaluation =
                            t4dapi.EvaluateAlarmState(
                                strAlarmMessage: strAlarmMessage,
                                env: runtimeEnvironment);

                        string strAlarmResponse =
                            alarmEvaluation.ResponseText;

                        Console.WriteLine(
                            $"{strTab1}{strAlarmResponse}");

                        Console.WriteLine(
                            $"{strTab1}Alarm severity: " +
                            $"{alarmEvaluation.PreviousSeverity} -> " +
                            $"{alarmEvaluation.CurrentSeverity}");

                        Console.WriteLine(
                            $"{strTab1}Red transition: " +
                            $"{alarmEvaluation.RedTransition}");
                        #endregion

                        #region Pause at Alarm Message

                        if (strStopAtAlarmMessage == "Yes")
                        {
                            string strPauseMessage =
                                "Time Window: " +
                                strBlockSizeHrs +
                                " hrs\nLatest value only: " +
                                strLatestValueOnly +
                                "\n\n" +
                                strAlarmMessage;

                            gnaT.pauseExecution(
                                strStopAtAlarmMessage,
                                strPauseMessage);
                        }
                        #endregion

                        #region Issue Daily Alarm Status Summary

                        string strDailyAlarmSummaryResponse =
                            t4dapi.IssueDailyAlarmStatusSummary(
                                strIssueDailyAlarmStatusSummary:
                                    strIssueDailyAlarmStatusSummary,
                                strTimeBlockType:
                                    strTimeBlockType,
                                emailCreds:
                                    emailCreds,
                                smsRecipients:
                                    smsRecipients,
                                env:
                                    runtimeEnvironment);

                        Console.WriteLine(
                            $"{strTab1}{strDailyAlarmSummaryResponse}");

                        #endregion

                        #region Determine Notification Requirements

                        string strNormalisedTimeBlockType =
                            strTimeBlockType.Trim();

                        bool blnTransmitForTimeBlock =
                            strNormalisedTimeBlockType.Equals(
                                value: "Manual",
                                comparisonType:
                                    StringComparison.OrdinalIgnoreCase) ||
                            strNormalisedTimeBlockType.Equals(
                                value: "Schedule",
                                comparisonType:
                                    StringComparison.OrdinalIgnoreCase);

                        bool alarmNotificationRequired =
                            alarmEvaluation.AlarmNotificationRequired;

                        bool redRecipientNotificationRequired =
                            alarmEvaluation.RedNotificationRequired;

                        List<SmsRecipient> selectedSmsRecipients = new();

                        List<string> selectedSmsMobile = new();

                        HashSet<string> selectedSmsPhoneNumbers = new(
                            comparer: StringComparer.Ordinal);

                        if (alarmNotificationRequired)
                        {
                            for (int iRecipientIndex = 0;
                                iRecipientIndex < smsRecipients.Count;
                                iRecipientIndex++)
                            {
                                SmsRecipient smsRecipient =
                                    smsRecipients[iRecipientIndex]
                                    ?? throw new InvalidDataException(
                                        message:
                                            $"SMS recipient index " +
                                            $"{iRecipientIndex} is null.");

                                bool blnRecipientIsEligible =
                                    smsRecipient.NotificationGroup ==
                                        SmsNotificationGroup.All ||
                                    (redRecipientNotificationRequired &&
                                     smsRecipient.NotificationGroup ==
                                        SmsNotificationGroup.Red);

                                if (!blnRecipientIsEligible)
                                {
                                    continue;
                                }

                                if (!selectedSmsPhoneNumbers.Add(
                                    item: smsRecipient.PhoneNumber))
                                {
                                    continue;
                                }

                                selectedSmsRecipients.Add(
                                    item: smsRecipient);

                                selectedSmsMobile.Add(
                                    item: smsRecipient.PhoneNumber);
                            }
                        }

                        bool scheduledEmailRequired =
                            blnShouldSend;

                        bool emailRequired =
                            blnTransmitForTimeBlock &&
                            (scheduledEmailRequired ||
                             alarmNotificationRequired);

                        bool smsRequired =
                            blnTransmitForTimeBlock &&
                            alarmNotificationRequired &&
                            selectedSmsMobile.Count > 0;

                        Console.WriteLine(
                            $"{strTab1}Scheduled email required: " +
                            $"{scheduledEmailRequired}");

                        Console.WriteLine(
                            $"{strTab1}Alarm notification required: " +
                            $"{alarmNotificationRequired}");

                        Console.WriteLine(
                            $"{strTab1}Red-recipient notification required: " +
                            $"{redRecipientNotificationRequired}");

                        Console.WriteLine(
                            $"{strTab1}Email required: {emailRequired}");

                        Console.WriteLine(
                            $"{strTab1}SMS required: {smsRequired}");

                        Console.WriteLine(
                            $"{strTab1}Selected SMS recipients: " +
                            $"{selectedSmsMobile.Count}");

                        #endregion

                        #region Create Export Workbook

                        bool exportFileCreated = false;
                        string strExportFileResult =
                            "Export workbook not required.";

                        if (emailRequired)
                        {
                            Console.WriteLine(
                                $"{strTab1}Create the export workbook");

                            try
                            {
                                if (string.IsNullOrWhiteSpace(
                                    value: strMasterWorkbookFullPath))
                                {
                                    throw new ArgumentException(
                                        message:
                                            "The master workbook path is required.",
                                        paramName:
                                            nameof(strMasterWorkbookFullPath));
                                }

                                if (string.IsNullOrWhiteSpace(
                                    value: strExportFile))
                                {
                                    throw new ArgumentException(
                                        message:
                                            "The export workbook path is required.",
                                        paramName: nameof(strExportFile));
                                }

                                if (!File.Exists(
                                    path: strMasterWorkbookFullPath))
                                {
                                    throw new FileNotFoundException(
                                        message:
                                            "The master workbook was not found.",
                                        fileName:
                                            strMasterWorkbookFullPath);
                                }

                                File.Copy(
                                    sourceFileName:
                                        strMasterWorkbookFullPath,
                                    destFileName: strExportFile,
                                    overwrite: true);

                                if (!File.Exists(path: strExportFile))
                                {
                                    throw new IOException(
                                        message:
                                            "The export workbook was not " +
                                            "created successfully.");
                                }

                                exportFileCreated = true;
                                strExportFileResult =
                                    "Export workbook created successfully.";

                                Console.WriteLine(
                                    $"{strTab2}{strExportFile}");

                                if (strAlarmVersion.Equals(
                                    value: "No",
                                    comparisonType:
                                        StringComparison.OrdinalIgnoreCase))
                                {
                                    Console.WriteLine(
                                        $"{strTab2}Hide " +
                                        $"{strReferenceWorksheet}");

                                    gnaSpreadsheetAPI.hideWorksheet(
                                        strExportFile,
                                        strReferenceWorksheet);

                                    Console.WriteLine(
                                        $"{strTab2}Hide " +
                                        $"{strAlarmsWorksheet}");

                                    gnaSpreadsheetAPI.hideWorksheet(
                                        strExportFile,
                                        strAlarmsWorksheet);

                                    Console.WriteLine(
                                        $"{strTab2}Hide " +
                                        $"{strSurveyWorksheet}");

                                    gnaSpreadsheetAPI.hideWorksheet(
                                        strExportFile,
                                        strSurveyWorksheet);
                                }

                                Console.WriteLine(
                                    $"{strTab1}Done");
                            }
                            catch (Exception ex)
                            {
                                exportFileCreated = false;

                                strExportFileResult =
                                    $"Export workbook creation failed: " +
                                    $"{ex.Message}";

                                Console.WriteLine(
                                    $"{strTab2}{strExportFileResult}");
                            }
                        }
                        else
                        {
                            Console.WriteLine(
                                $"{strTab1}Export workbook not required");
                        }

                        #endregion

                        #region Trigger Levels

                        string strTriggerHeader =
                            "\n\n" +
                            "LIMITING CRITERIA FOR SHORT TWIST " +
                            "(3m baseline)\n" +
                            "Twist < 1 in 500: 500\n" +
                            "Twist between 1 in 500 and 1 in 250: 250\n" +
                            "Twist > 1 in 250: 0\n" +
                            "\n" +
                            "LIMITING CRITERIA FOR LONG TWIST " +
                            "(15m baseline)\n" +
                            "Warp < 1 in 800: 800\n" +
                            "Warp between 1 in 400 and 1 in 800: 400\n" +
                            "Warp > 1 in 400: 0\n" +
                            "\n" +
                            "LIMITING CRITERIA FOR TOP\n" +
                            "Top < 7.5 over 6m: 0\n" +
                            "Top between 7.5 and 10: 7.5\n" +
                            "Top over 10mm: 10\n";

                        #endregion

                        #region Initialise Transmission Results

                        bool emailAttempted = false;
                        bool emailSuccess = false;

                        string strEmailTransmissionResult =
                            emailRequired
                                ? "Email not attempted."
                                : blnTransmitForTimeBlock
                                    ? "Email not required."
                                    : "Email not attempted: Historic time block.";

                        bool smsAttempted = false;
                        bool smsSuccess = false;

                        string strSmsTransmissionResult =
                            smsRequired
                                ? "SMS not attempted."
                                : alarmNotificationRequired &&
                                  !blnTransmitForTimeBlock
                                    ? "SMS not attempted: Historic time block."
                                    : alarmNotificationRequired &&
                                      selectedSmsMobile.Count == 0
                                        ? "SMS not required: no recipients " +
                                          "are eligible for this alarm transition."
                                    : "SMS not required.";

                        #endregion



                        #region Transmit SMS

                        if (smsRequired)
                        {
                            smsAttempted = true;

                            try
                            {
                                string strSystemCredentialsFolderValue =
                                    runtimeEnvironment.SystemCredentialsFolder
                                    ?.Trim()
                                    ?? throw new InvalidOperationException(
                                        message:
                                            "SystemCredentialsFolder has " +
                                            "not been configured.");

                                if (strSystemCredentialsFolderValue.Length == 0)
                                {
                                    throw new InvalidOperationException(
                                        message:
                                            "SystemCredentialsFolder is empty.");
                                }

                                string strTwilioCredentialsFullPath =
                                    Path.Combine(
                                        path1:
                                            strSystemCredentialsFolderValue,
                                        path2:
                                            TwilioCredentialsFileName);

                                if (!File.Exists(
                                    path:
                                        strTwilioCredentialsFullPath))
                                {
                                    throw new FileNotFoundException(
                                        message:
                                            "The Twilio credentials file " +
                                            "was not found.",
                                        fileName:
                                            strTwilioCredentialsFullPath);
                                }

                                TimeZoneInfo projectTimeZone =
                                    TimeZoneInfo.FindSystemTimeZoneById(
                                        id: strTimeZoneID);

                                DateTimeOffset projectLocalTime =
                                    TimeZoneInfo.ConvertTime(
                                        dateTimeOffset:
                                            DateTimeOffset.UtcNow,
                                        destinationTimeZone:
                                            projectTimeZone);

                                string strSmsAlarmEvent =
                                    alarmEvaluation.RedTransition switch
                                    {
                                        RedAlarmTransition.EnteredRed =>
                                            "Alarm entered RED state",

                                        RedAlarmTransition.LeftRed =>
                                            "Alarm left RED state",

                                        _ => strAlarmResponse
                                    };

                                string strSmsTitleWithTime =
                                    $"{strSMSTitle}:" +
                                    $"{projectLocalTime:HH'h'mm}: " +
                                    $"{strSmsAlarmEvent}";

                                string strSmsBody =
                                    strAlarmResponse.StartsWith(
                                        value: "Alarm reset",
                                        comparisonType:
                                            StringComparison.Ordinal)
                                        ? "System returned to the " +
                                          "No Alarm state"
                                        : strAlarmMessage;

                                string strSmsMessage =
                                    $"{strSmsTitleWithTime}\n" +
                                    $"{strSmsBody}";

                                //Console.WriteLine(
                                //    $"\nSMS transmission" +
                                //    $"\nRecipients: " +
                                //    $"{string.Join(", ", selectedSmsMobile)}" +
                                //    $"\nTitle: {strSmsTitleWithTime}" +
                                //    $"\nMessage:\n{strSmsBody}");

                                smsSuccess = gnaT.sendSMSArray(
                                    strSMSmessage: strSmsMessage,
                                    smsMobile: selectedSmsMobile,
                                    strCredentialsFileFullPath:
                                        strTwilioCredentialsFullPath);

                                strSmsTransmissionResult =
                                    smsSuccess
                                        ? "SMS sent successfully"
                                        : "SMS transmission returned false";
                            }
                            catch (Exception ex)
                            {
                                smsSuccess = false;

                                strSmsTransmissionResult =
                                    $"SMS transmission exception: " +
                                    $"{ex.Message}";
                            }

                            Console.WriteLine(
                                $"{strTab1}" +
                                $"{strSmsTransmissionResult}");
                        }

                        #endregion

                        #region Transmit Email

                        string? originalSendEmail =
                            emailCreds.SendEmail;

                        string? originalEmailTransmissionDays =
                            emailCreds.EmailTransmissionDays;

                        string? originalEmailTransmissionTime =
                            emailCreds.EmailTransmissionTime;

                        string? originalEmailSubject =
                            emailCreds.Subject;

                        string? originalEmailBody =
                            emailCreds.Body;

                        List<string>? originalEmailAttachments =
                            emailCreds.Attachments;

                        string strEmailSubjectUsed = string.Empty;

                        if (emailRequired)
                        {
                            if (!exportFileCreated)
                            {
                                strEmailTransmissionResult =
                                    "Email not attempted because the " +
                                    $"{strExportFileResult}";
                            }
                            else
                            {
                                emailAttempted = true;

                                try
                                {
                                    string strEmailSubjectBase =
                                        $"{strReportType} Report: " +
                                        $"{strProjectTitle} " +
                                        $"({strReportTime})";

                                    if (alarmNotificationRequired)
                                    {
                                        emailCreds.SendEmail = "Yes";
                                        emailCreds.EmailTransmissionDays =
                                            "All";
                                        emailCreds.EmailTransmissionTime =
                                            "Now";

                                        strEmailSubjectUsed =
                                            $"{strEmailSubjectBase} " +
                                            $"({strSMSTitle}: " +
                                            $"{strAlarmResponse})";

                                        emailCreds.Body =
                                            strAlarmResponse.StartsWith(
                                                value: "Alarm reset",
                                                comparisonType:
                                                    StringComparison.Ordinal)
                                                ? "System returned to the " +
                                                  "No Alarm state"
                                                : strAlarmMessage;
                                    }
                                    else
                                    {
                                        strEmailSubjectUsed =
                                            strEmailSubjectBase;

                                        string strSubMessage =
                                            t4dapi.extractMinTrackGeometry(
                                                trackPairList:
                                                    trackPairList,
                                                prismList:
                                                    prismList);

                                        string missingPrisms =
                                            t4dapi.ExtractMissingPrisms(
                                                prismList);

                                        // Retained for later email-body
                                        // integration.
                                        strSubMessage =
                                            strSubMessage +
                                            missingPrisms;

                                        string strScheduledEmailMessage =
                                            $"\nThis is an automated " +
                                            $"{strReportType} report for " +
                                            $"contract {strContractTitle}." +
                                            $"\nPlease do not reply to " +
                                            $"this email.";

                                        strScheduledEmailMessage +=
                                            strTriggerHeader +
                                            "\nPRISMS IN ALARM STATE\n" +
                                            strAlarmMessage +
                                            "\n";

                                        emailCreds.Body =
                                            strScheduledEmailMessage;
                                    }

                                    emailCreds.Subject =
                                        strEmailSubjectUsed;

                                    emailCreds.Body =
                                        gnaT.addCopyright(
                                            "Track Geometry Report",
                                            emailCreds.Body
                                            ?? string.Empty);

                                    emailCreds.Attachments =
                                        new List<string>
                                        {
                                            strExportFile
                                        };

                                    //Console.WriteLine(
                                    //    $"\nEmail transmission" +
                                    //    $"\nRecipients: " +
                                    //    $"{emailCreds.EmailRecipients ?? "<not configured>"}" +
                                    //    $"\nSubject: " +
                                    //    $"{emailCreds.Subject ?? "<not configured>"}" +
                                    //    $"\nMessage:\n" +
                                    //    $"{emailCreds.Body ?? "<empty>"}");

                                    strEmailTransmissionResult =
                                        gnaT.TransmitEmail(
                                            emailCredentials:
                                                emailCreds);

                                    emailSuccess = string.Equals(
                                        a:
                                            strEmailTransmissionResult,
                                        b:
                                            EmailTransmissionSuccess,
                                        comparisonType:
                                            StringComparison.Ordinal);
                                }
                                catch (Exception ex)
                                {
                                    emailSuccess = false;

                                    strEmailTransmissionResult =
                                        $"Email transmission exception: " +
                                        $"{ex.Message}";
                                }
                                finally
                                {
                                    emailCreds.SendEmail =
                                        originalSendEmail;

                                    emailCreds.EmailTransmissionDays =
                                        originalEmailTransmissionDays;

                                    emailCreds.EmailTransmissionTime =
                                        originalEmailTransmissionTime;

                                    emailCreds.Subject =
                                        originalEmailSubject;

                                    emailCreds.Body =
                                        originalEmailBody;

                                    emailCreds.Attachments =
                                        originalEmailAttachments;
                                }
                            }

                            Console.WriteLine(
                                $"{strTab2}" +
                                $"{strEmailTransmissionResult}");
                        }

                        #endregion

                        #region Update Alarm State File

                        bool alarmStateFileUpdated = false;
                        string strAlarmStateFileResult =
                            "Alarm-state file not updated.";

                        bool alarmStateUnchanged =
                            alarmEvaluation.StateChange ==
                            AlarmStateChange.Unchanged;

                        bool alarmStateUpdateRequired =
                            alarmStateUnchanged ||
                            (alarmNotificationRequired &&
                             emailSuccess);

                        if (alarmStateUpdateRequired)
                        {
                            try
                            {
                                strAlarmStateFileResult =
                                    t4dapi.UpdateAlarmStateFile(
                                        strAlarmMessage:
                                            strAlarmMessage,
                                        env:
                                            runtimeEnvironment);

                                alarmStateFileUpdated = true;
                            }
                            catch (Exception ex)
                            {
                                alarmStateFileUpdated = false;

                                strAlarmStateFileResult =
                                    $"Alarm-state file update failed: " +
                                    $"{ex.Message}";
                            }
                        }

                        Console.WriteLine(
                            $"{strTab1}" +
                            $"{strAlarmStateFileResult}");

                        #endregion

                        #region Write Consolidated Alarm Activity Log

                        bool activityLogWritten = false;
                        string strActivityLogResult =
                            "Activity log not required.";

                        if (alarmNotificationRequired)
                        {
                            try
                            {
                                TimeZoneInfo projectTimeZone =
                                    TimeZoneInfo.FindSystemTimeZoneById(
                                        id: strTimeZoneID);

                                DateTimeOffset projectLocalTime =
                                    TimeZoneInfo.ConvertTime(
                                        dateTimeOffset:
                                            DateTimeOffset.UtcNow,
                                        destinationTimeZone:
                                            projectTimeZone);

                                string strLocalActivityTimestamp =
                                    projectLocalTime.ToString(
                                        format:
                                            "yyyy-MM-dd HH:mm:ss zzz",
                                        formatProvider:
                                            CultureInfo.InvariantCulture);

                                string strEmailRecipientsForLog =
                                    emailCreds.EmailRecipients
                                    ?? string.Empty;

                                string strConfiguredSmsRecipientsForLog =
                                    smsRecipients.Count == 0
                                        ? string.Empty
                                        : string.Join(
                                            separator: ", ",
                                            values:
                                                smsRecipients.Select(
                                                    selector: recipient =>
                                                        $"{recipient.ConfigurationKey}=" +
                                                        $"{recipient.PhoneNumber}" +
                                                        $"[{recipient.NotificationGroup}]"));

                                string strSelectedSmsRecipientsForLog =
                                    selectedSmsRecipients.Count == 0
                                        ? string.Empty
                                        : string.Join(
                                            separator: ", ",
                                            values:
                                                selectedSmsRecipients.Select(
                                                    selector: recipient =>
                                                        $"{recipient.PhoneNumber}" +
                                                        $"[{recipient.NotificationGroup}]"));

                                StringBuilder activityLogEntry =
                                    new StringBuilder();

                                activityLogEntry.AppendLine(
                                    value:
                                        $"[{strLocalActivityTimestamp}] " +
                                        $"{strAlarmResponse}");

                                activityLogEntry.AppendLine(
                                    value:
                                        $"Time block type: " +
                                        $"{strTimeBlockType}");

                                activityLogEntry.AppendLine(
                                    value:
                                        $"Previous alarm severity: " +
                                        $"{alarmEvaluation.PreviousSeverity}");

                                activityLogEntry.AppendLine(
                                    value:
                                        $"Current alarm severity: " +
                                        $"{alarmEvaluation.CurrentSeverity}");

                                activityLogEntry.AppendLine(
                                    value:
                                        $"Alarm state change: " +
                                        $"{alarmEvaluation.StateChange}");

                                activityLogEntry.AppendLine(
                                    value:
                                        $"Red alarm transition: " +
                                        $"{alarmEvaluation.RedTransition}");

                                activityLogEntry.AppendLine(
                                    value:
                                        $"Configured SMS recipients: " +
                                        $"{strConfiguredSmsRecipientsForLog}");

                                activityLogEntry.AppendLine(
                                    value:
                                        $"Selected SMS recipients: " +
                                        $"{strSelectedSmsRecipientsForLog}");

                                activityLogEntry.AppendLine(
                                    value:
                                        $"Email required: " +
                                        $"{emailRequired}");

                                activityLogEntry.AppendLine(
                                    value:
                                        $"Email attempted: " +
                                        $"{emailAttempted}");

                                string strEmailOutcome =
                                    emailAttempted
                                        ? emailSuccess
                                            ? "Success"
                                            : "Failed"
                                        : "Not attempted";

                                activityLogEntry.AppendLine(
                                    value:
                                        $"Email transmission: " +
                                        $"{strEmailOutcome}");

                                activityLogEntry.AppendLine(
                                    value:
                                        $"Email recipients: " +
                                        $"{strEmailRecipientsForLog}");

                                activityLogEntry.AppendLine(
                                    value:
                                        $"Email result: " +
                                        $"{strEmailTransmissionResult}");

                                activityLogEntry.AppendLine(
                                    value:
                                        $"SMS required: " +
                                        $"{smsRequired}");

                                activityLogEntry.AppendLine(
                                    value:
                                        $"SMS attempted: " +
                                        $"{smsAttempted}");

                                string strSmsOutcome =
                                    smsAttempted
                                        ? smsSuccess
                                            ? "Success"
                                            : "Failed"
                                        : "Not attempted";

                                activityLogEntry.AppendLine(
                                    value:
                                        $"SMS transmission: " +
                                        $"{strSmsOutcome}");

                                activityLogEntry.AppendLine(
                                    value:
                                        $"SMS recipients: " +
                                        $"{strSelectedSmsRecipientsForLog}");

                                activityLogEntry.AppendLine(
                                    value:
                                        $"SMS result: " +
                                        $"{strSmsTransmissionResult}");

                                activityLogEntry.AppendLine(
                                    value:
                                        $"Alarm-state file updated: " +
                                        $"{alarmStateFileUpdated}");

                                activityLogEntry.AppendLine(
                                    value:
                                        $"Alarm-state file result: " +
                                        $"{strAlarmStateFileResult}");

                                activityLogEntry.AppendLine();

                                string strSystemActivityLogFullPath =
                                    Path.Combine(
                                        path1: strSystemLogsFolder,
                                        path2:
                                            SystemActivityLogFileName);

                                File.AppendAllText(
                                    path:
                                        strSystemActivityLogFullPath,
                                    contents:
                                        activityLogEntry.ToString());

                                activityLogWritten = true;
                                strActivityLogResult =
                                    "Activity log written.";
                            }
                            catch (Exception ex)
                            {
                                activityLogWritten = false;

                                strActivityLogResult =
                                    $"Activity log failed: " +
                                    $"{ex.Message}";
                            }

                            Console.WriteLine(
                                $"{strTab1}" +
                                $"{strActivityLogResult}");
                        }

                        #endregion

                        #region Echo Transmission Summary

                        if (emailRequired ||
                            smsRequired ||
                            alarmNotificationRequired)
                        {
                            string strActivityLogSummary =
                                alarmNotificationRequired
                                    ? activityLogWritten
                                        ? "Written"
                                        : "Failed"
                                    : "Not required";

                            //Console.WriteLine(
                            //    $"{strTab1}Notification summary: " +
                            //    $"Email={strEmailTransmissionResult}; " +
                            //    $"SMS={strSmsTransmissionResult}; " +
                            //    $"AlarmStateUpdated=" +
                            //    $"{alarmStateFileUpdated}; " +
                            //    $"ActivityLog=" +
                            //    $"{strActivityLogSummary}");
                        }
                        else
                        {
                            Console.WriteLine(
                                $"{strTab1}No email or SMS required");
                        }

                        #endregion

                        Console.WriteLine($"{strTab1}Time block completed\n");
                    }
                }

#endregion


ThatsAllFolks:

                FinishAndExit(strReportType);

            }
            catch (Exception ex)
            {
                try
                {
                    string? strFatalCrashFolder =
                        Path.GetDirectoryName(
                            path: strFatalCrashLogFullPath);

                    if (!string.IsNullOrWhiteSpace(
                        value: strFatalCrashFolder))
                    {
                        Directory.CreateDirectory(
                            path: strFatalCrashFolder);
                    }

                    File.WriteAllText(
                        path: strFatalCrashLogFullPath,
                        contents: ex.ToString());
                }
                catch
                {
                    // The original exception remains the primary failure.
                }

                Console.Error.WriteLine(ex.ToString());
                Environment.ExitCode = 1;
            }


        }



        #region Config helpers
        static string CleanConfig(string s) => (s ?? string.Empty).Trim().Trim('\'', '"');

        static string GetRequired(NameValueCollection cfg, string key)
        {
            string v = CleanConfig(cfg[key]);
            if (v.Length == 0)
                throw new ConfigurationErrorsException($"\nMissing/empty config key '{key}'.");
            return v;
        }

        static int GetRequiredInt(NameValueCollection cfg, string key, int minValueInclusive = int.MinValue, int maxValueInclusive = int.MaxValue)
        {
            string s = GetRequired(cfg, key);
            if (!int.TryParse(s, NumberStyles.Integer, CultureInfo.InvariantCulture, out int v))
                throw new ConfigurationErrorsException($"\nConfig key '{key}' is invalid (expected integer). Value='{s}'.");
            if (v < minValueInclusive || v > maxValueInclusive)
                throw new ConfigurationErrorsException($"\nConfig key '{key}' is out of range. Value={v}.");
            return v;
        }

        static bool IsYes(string s) => string.Equals(CleanConfig(s), "Yes", StringComparison.OrdinalIgnoreCase);
        #endregion


        #region Internal helpers
        internal static class ConfigParsing
        {
            public static bool GetBoolYesNo(System.Collections.Specialized.NameValueCollection appSettings, string key)
            {
                #region Read raw setting
                string? raw = appSettings[key];
                #endregion

                #region Validate missing value
                if (raw is null)
                {
                    string message = $"\nMissing required appSetting: '{key}'.";
                    ConfigurationErrorsException exception = new(message);

                    Console.Error.WriteLine(exception.Message);

                    throw exception;
                }
                #endregion

                #region Normalise value
                string value = raw.Trim();
                #endregion

                #region Validate Yes/No
                if (value.Equals("Yes", StringComparison.OrdinalIgnoreCase))
                {
                    return true;
                }

                if (value.Equals("No", StringComparison.OrdinalIgnoreCase))
                {
                    return false;
                }
                #endregion

                #region Throw invalid value error
                {
                    string message = $"Invalid value for appSetting '{key}': '{raw}'. Expected 'Yes' or 'No'.";
                    ConfigurationErrorsException exception = new(message);

                    Console.Error.WriteLine(exception.Message);

                    throw exception;
                }
                #endregion
            }

            public static string GetRequiredString(System.Collections.Specialized.NameValueCollection appSettings, string key)
            {
                #region Read raw setting
                string? raw = appSettings[key];
                #endregion

                #region Validate required value
                if (string.IsNullOrWhiteSpace(raw))
                {
                    string message = $"\nMissing or empty required appSetting: '{key}'.";
                    Console.Error.WriteLine(message);
                    throw new ConfigurationErrorsException(message: message);
                }
                #endregion

                #region Return normalised value
                return raw.Trim();
                #endregion
            }

            public static int GetRequiredInt(System.Collections.Specialized.NameValueCollection appSettings, string key)
            {
                string raw = GetRequiredString(appSettings, key);
                if (!int.TryParse(raw, out int value))
                    throw new ConfigurationErrorsException($"\nInvalid integer for appSetting '{key}': '{raw}'.");
                return value;
            }

            public static double GetRequiredDouble(System.Collections.Specialized.NameValueCollection appSettings, string key)
            {
                string raw = GetRequiredString(appSettings, key);
                if (!double.TryParse(
                        raw,
                        System.Globalization.NumberStyles.Float,
                        System.Globalization.CultureInfo.InvariantCulture,
                        out double value))
                {
                    throw new ConfigurationErrorsException(
                        $"\nInvalid double for appSetting '{key}': '{raw}'. Use '.' as decimal separator.");
                }
                return value;
            }
        }
        #endregion
    }
}
