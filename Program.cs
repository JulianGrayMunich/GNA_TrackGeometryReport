using System.Collections.Specialized;
using System.ComponentModel;
using System.Configuration;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Reflection.Metadata.Ecma335;
using System.Runtime.InteropServices;

using databaseAPI;

using EASendMail;

using GNA_CommercialLicenseValidator;

using gnaDataClasses;

using GNAgeneraltools;

using GNAspreadsheettools;

using GNAsurveytools;

using Microsoft.Data.SqlClient;

using OfficeOpenXml;

using T4Dlibrary;

using Twilio.Rest.Api.V2010.Account;
using Twilio.Rest.Sync.V1.Service.SyncStream;
using Twilio.TwiML.Messaging;
using Twilio.TwiML.Voice;

using static T4Dlibrary.T4Dapi;







namespace TrackGeometryReport
{
    class Program
    {
        static void Main()
        {
            // This is a generic and expanded version of the SPN010 track geometry reports
            // additional featureds are added to make it more user friendly.
            // 20260412



            try
            {


#pragma warning disable CS0162
#pragma warning disable CS8600
#pragma warning disable CS8601
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
                string strTab3 = "           ";

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
                string? strHistoricDhWorksheet = config["HistoricDhWorksheet"]?.Trim();
                string? strHistoricTopWorksheet = config["HistoricTopWorksheet"]?.Trim();
                string? strHistoricdHWorksheet = config["HistoricdHWorksheet"]?.Trim();
                string? strHistoricTwistWorksheet = config["HistoricTwistWorksheet"]?.Trim();
                string? strHistoricLongTwistWorksheet = config["HistoricLongTwistWorksheet"]?.Trim();
                string? strAlarmsWorksheet = config["AlarmsWorksheet"]?.Trim();
                string? strHistoricCantWorksheet = config["HistoricCantWorksheet"]?.Trim(); 
                string? strLatestTiltWorksheet = config["LatestTiltWorksheet"]?.Trim();
                string? strHistoricTiltWorksheet = config["HistoricTiltWorksheet"]?.Trim();
                string? strHistoricDeltaTiltWorksheet = config["HistoricDeltaTiltWorksheet"]?.Trim();
                string? strHistoricDeltaTiltAWorksheet = config["HistoricDeltaTiltAWorksheet"]?.Trim();
                string? strHistoricDeltaTiltBWorksheet = config["HistoricDeltaTiltBWorksheet"]?.Trim();
                string? strHistoricDeltaTiltCWorksheet = config["HistoricDeltaTiltCWorksheet"]?.Trim();
                string? strLatestExtensometerWorksheet = config["LatestExtensometerWorksheet"]?.Trim();
                string? strHistoricExtensometerWorksheet = config["HistoricExtensometerWorksheet"]?.Trim();
                string? strHistoricDeltaExtensometerWorksheet = config["HistoricDeltaExtensometerWorksheet"]?.Trim();
                string? strReportSpec = config["ReportSpec"]?.Trim();
                string? strWorkbookPassword = config["WorkbookPassword"]?.Trim();

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
                string strAlarmVersion = config["AlarmVersion"];
                string strDeleteMissingValues = config["DeleteMissingValues"];
                string strLatestValueOnly = config["LatestValueOnly"];
                string strRecordHistoricData = config["recordHistoricData"];
                #endregion

                #region Report variables
                string strContractTitle = ConfigParsing.GetRequiredString(config, "ContractTitle");
                string strReportType = ConfigParsing.GetRequiredString(config, "ReportType");
                string strIncludeHistoricTwist = config["includeHistoricTwist"];
                string strIncludeHistoricSettlement = config["includeHistoricSettlement"];
                string strIncludeHistoricTop = config["includeHistoricTop"];
                string strIncludeMissingTargets = config["includeMissingTargets"];


                strReportType = ConfigParsing.GetRequiredString(config, "ReportType");





                #endregion

                #region System variables
                string strMasterWorkbookFullPath = strExcelPath + strExcelFile;
                Console.WriteLine($"{strTab1}Done");
                #endregion

                #region General variables

                Console.WriteLine($"{strTab1}General variables");

                string strComputeMeanDeltas = CleanConfig(config["computeMean"]);
                if (strComputeMeanDeltas.Length == 0) strComputeMeanDeltas = "No";

                string strUpdateSensorList = CleanConfig(config["updateSensorList"]);
                if (strUpdateSensorList.Length == 0) strUpdateSensorList = "No";

                string strSystemLogsFolder = CleanConfig(config["SystemStatusFolder"]);
                if (strSystemLogsFolder.Length == 0) strSystemLogsFolder = @"C:\__SystemLogs\";

                string strAlarmfolder = CleanConfig(config["SystemAlarmFolder"]);
                if (strAlarmfolder.Length == 0) strAlarmfolder = @"C:\__SystemAlarms\";

                Directory.CreateDirectory(strSystemLogsFolder);
                Directory.CreateDirectory(strAlarmfolder);

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

                string? strSPN010alarms = config["AlarmNotifications"];

                double dblTimeZoneOffset = gnaDBAPI.getProjectTimeZoneOffset(strDBconnection, strProjectTitle);

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
                    strSystemLogsFolder: strSystemLogsFolder);
                #endregion

                #region SMS settings
                Console.WriteLine($"{strTab1}SMS settings");

                string? strSMSTitle = config["SMSTitle"]?.Trim();
                string strMobileList = "";

                List<string> smsMobile = new();
                foreach (string key in config.AllKeys.Where(k =>
                             !string.IsNullOrWhiteSpace(k) &&
                             k.StartsWith("RecipientPhone", StringComparison.OrdinalIgnoreCase)))
                {
                    string phoneNumber = gnaT.NormalizePhoneNumber(config[key], key);
                    smsMobile.Add(phoneNumber);

                    if (strMobileList.Length > 0) strMobileList += ",";
                    strMobileList += phoneNumber;
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
                string logFileMessage = "";

                // ---- Working strings and file paths ----
                string strTempString = "";
                string strMasterFile = "";
                string strWorkingFile = "";
                string strExportFile = "";

                // ---- Row and column positions ----
                int iRow = Convert.ToInt32(strFirstDataRow);
                int iReferenceFirstDataRow = Convert.ToInt32(strFirstDataRow);


                Console.WriteLine($"{strTab1}Assigned");
                #endregion

                #region populate the RuntimeEnvironment class

                gnaDataClasses.RuntimeEnvironment runtimeEnvironment = new()
                {
                    // ---- Database ----
                    DbConnectionString = strDBconnection,
                    ProjectTitle = strProjectTitle,
                    ReportType = strReportType,


                    // ---- Workbook ----
                    ExcelPath = strExcelPath,
                    ExcelFile = strExcelFile,

                    // ---- Permissions ----
                    RecordHistoricData = strRecordHistoricData,


                    // ---- Worksheets ----
                    ReferenceWorksheet = strReferenceWorksheet,
                    SurveyWorksheet = strSurveyWorksheet,
                    TrackGeometryWorksheet = strTrackGeometryWorksheet,
                    HistoricCantWorksheet = strHistoricCantWorksheet,
                    HistoricTopWorksheet = strHistoricTopWorksheet,
                    HistoricdHWorksheet = strHistoricdHWorksheet,
                    HistoricTwistWorksheet = strHistoricTwistWorksheet,
                    HistoricLongTwistWorksheet = strHistoricLongTwistWorksheet, 

                    // ---- Row/Col configuration ----
                    FirstDataRow = iFirstDataRow,
                    FirstDataCol = iFirstDataCol,
                    FirstOutputRow = iFirstOutputRow,
                    FirstTrackRow = iFirstTrackRow
                };

                #endregion

                #region Clean exit
                void FinishAndExit()
                {
                    Console.WriteLine("\nSensor report completed...\n\n");
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
                    gnaSpreadsheetAPI.checkWorksheetExists(strMasterWorkbookFullPath, strHistoricCantWorksheet);
                    gnaSpreadsheetAPI.checkWorksheetExists(strMasterWorkbookFullPath, strHistoricTwistWorksheet);
                    gnaSpreadsheetAPI.checkWorksheetExists(strMasterWorkbookFullPath, strHistoricLongTwistWorksheet);
                    gnaSpreadsheetAPI.checkWorksheetExists(strMasterWorkbookFullPath, strHistoricTwistWorksheet);
                    gnaSpreadsheetAPI.checkWorksheetExists(strMasterWorkbookFullPath, strHistoricLongTwistWorksheet);
                    gnaSpreadsheetAPI.checkWorksheetExists(strMasterWorkbookFullPath, strHistoricTopWorksheet);
                    gnaSpreadsheetAPI.checkWorksheetExists(strMasterWorkbookFullPath, strHistoricdHWorksheet);
                    gnaSpreadsheetAPI.checkWorksheetExists(strMasterWorkbookFullPath, strAlarmsWorksheet);
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
                        Console.WriteLine("\nError in Timeblock Type");
                        Console.WriteLine("Time block type: " + strTimeBlockType);
                        Console.WriteLine("Must be Manual, Schedule or Historic");
                        Console.WriteLine("\nPress key to exit...");
                        Console.ReadKey();
                        Environment.Exit(1);
                        break;
                }

                string strTimeStampLocal;
                if (strTimeBlockType == "Manual")
                {
                    string strTemp = strEmailTime.Replace(":", "").Replace("-", "").Replace(" ", "_");
                    strExportFile = strExcelPath + strContractTitle + "_" + strReportType + "_" + strTemp + ".xlsx";
                    strWorkingFile = strExportFile;
                    strMasterFile = strExcelPath + strExcelFile;
                    strTimeStampLocal = strTemp;
                }
                else
                {
                    strExportFile = strExcelPath + strContractTitle + "_" + strReportType + "_" + "DateTime" + ".xlsx";
                    strWorkingFile = strExportFile;
                    strMasterFile = strExcelPath + strExcelFile;
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
                        $"computeMean='{strcomputeMeans}' | " +
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
                        strReportTime = t4dapi.prepareReportTime(strTimeBlockEndLocal);
                        strReportTime += timeBlockSuffix;
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
                        strExportFile = strExcelPath + strContractTitle + "_" + strReportType + "_" + strReportTime + timeBlockSuffix + ".xlsx";
                        #endregion

                        #region Preparing Track Geometry data
                        // read deltas from the db for the current time block
                        List<Points> currentDeltas = t4dapi.GetAllPointsMeanDeltas(
                            dbConnection: strDBconnection,
                            projectTitle: strProjectTitle,
                            timeBlockStartUTC: strTimeBlockStartUTC,
                            timeBlockEndUTC: strTimeBlockEndUTC,
                            iTimeIntervalHours: null);

                        currentDeltas = t4dapi.removeOutliers(
                            pointsList: currentDeltas,
                            checkDistance: 0.3);

                        prismList = t4dapi.combinePointsLists(parentList: prismList, childList: currentDeltas);

                        prismList = t4dapi.removeOutliers(
                            pointsList: prismList,
                            checkDistance: 0.3);
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
                            strTimeBlockEndUTC: strTimeBlockEndUTC);

                        #endregion

                        //Verified: prismList is OK



                        #region Create export file
                        Console.WriteLine($"{strTab1}Create export file");
                        strExportFile = strExcelPath + strContractTitle + "_" + strReportType + "_" + strReportTime + ".xlsx";
                        if (string.IsNullOrWhiteSpace(strMasterWorkbookFullPath))
                            throw new ArgumentException("strMasterWorkbookFullPath is required.", nameof(strMasterWorkbookFullPath));

                        if (string.IsNullOrWhiteSpace(strExportFile))
                            throw new ArgumentException("strExportFile is required.", nameof(strExportFile));

                        if (!File.Exists(path: strMasterWorkbookFullPath))
                            throw new FileNotFoundException("Master workbook not found.", strMasterWorkbookFullPath);

                        try
                        {
                            File.Copy(
                                sourceFileName: strMasterWorkbookFullPath,
                                destFileName: strExportFile,
                                overwrite: true);
                            Console.WriteLine($"{strTab2}{strExportFile}");
                        }
                        catch (Exception ex)
                        {
                            string strMessage1 =
                                $"\nFailed to copy master workbook from '{strMasterWorkbookFullPath}' to '{strExportFile}'. {ex.Message}";
                            Console.WriteLine(strMessage1);
                            throw new InvalidOperationException(strMessage1, ex);
                        }

                        #endregion

                        #region Prepare export file for distribution
                        Console.WriteLine($"{strTab1}Prepare the export workbook");
                        Console.WriteLine($"{strTab2}Hide {strReferenceWorksheet}");
                        gnaSpreadsheetAPI.hideWorksheet(strExportFile, strReferenceWorksheet);
                        Console.WriteLine($"{strTab2}Hide {strAlarmsWorksheet}");
                        gnaSpreadsheetAPI.hideWorksheet(strExportFile, strAlarmsWorksheet);
                        Console.WriteLine($"{strTab2}Hide {strSurveyWorksheet}");
                        gnaSpreadsheetAPI.hideWorksheet(strExportFile, strSurveyWorksheet);
                        //Console.WriteLine($"{strTab2}Freeze {strExportFile}");
                        //gnaSpreadsheetAPI.freezeWorkbook(strExportFile, strWorkbookPassword);
                        Console.WriteLine($"{strTab1}Done");
                        #endregion

                #endregion

                #region Trigger levels
                        string strTriggerHeader =

                            "\n\n" +
                            "LIMITING CRITERIA FOR SHORT TWIST (3m baseline)\n" +
                            "Twist < 1 in 500: 500\n" +
                            "Twist between 1 in 500 and 1 in 250: 250\n" +
                            "Twist > 1 in 250: 0\n" +
                            "\n" +
                            "LIMITING CRITERIA FOR LONG TWIST (15m baseline)\n" +
                            "Warp < 1 in 800: 800\n" +
                            "Warp between 1 in 400 and 1 in 800: 400\n" +
                            "Warp > 1 in 400: 0\n" +
                            "\n" +
                            "LIMITING CRITERIA FOR TOP\n" +
                            "Top < 7.5 over 6m: 0\n" +
                            "Top between 7.5 and 10: 7.5\n" +
                            "Top over 10mm: 10\n";

                        #endregion


                 #region Top,twist, missing targets alarms
                        Console.WriteLine($"{headingNo++}. Top,Twist,Long Twist, missing targets alarm state & SMS if alarms");

                        // first populate the alarm worksheet "Alarms"
                        //  Top alarm: Col B: =IF(ABS(I9)>=$V$22,"Red",IF(ABS(I9)>=$V$21,"Amber",IF(ABS(I9)>=$V$20,"OK","")))
                        //  Short Twist Alarm: Col C: =IF(L9>=$V$8,"OK",IF(L9>=$V$9,"Amber",IF(L9>=$V$10,"Red","")))
                        //  Long Twist Alarm: Col G: =IF(P13>=$V$14,"OK",IF(P13>=$V$15,"Amber",IF(P13>=$V$16,"Red","")))
                        //  Col AD: =IF(OR(B8="",B8=0),"",B8)
                        //  repeat for columns AH and AI

                        // generate the alarm message
                        string strAlarmMessage = gnaSpreadsheetAPI.SPN010AlarmState(
                            strMasterFile,
                            strAlarmsWorksheet,
                            iFirstTrackRow,
                            strIncludeMissingTargets);

                        string strTimeNow = DateTime.Now.ToString("HH'h'mm");
                        string strTempMessage = strSMSTitle + ":" + strTimeNow + "\n" + strAlarmMessage;

                        string strMessage = "Time Window: " + strBlockSizeHrs + " hrs\nLatest value only: " + strLatestValueOnly + "\n\n" + strAlarmMessage;

                       

                        gnaT.pauseExecution(strStopAtAlarmMessage, strMessage);

                        
                        if (strAlarmMessage != "No Alarm")
                        {
                            if (strStopAtAlarmMessage == "No")
                            {
                                Console.WriteLine($"\n{strTab1}Alarms detected:\n");
                                Console.WriteLine($"{strAlarmMessage}\n"); // multiline causes odd output alignment in console
                            }

                            string SMSmessage = strSMSTitle + ":" + strTimeNow + "\n" + strAlarmMessage;


                            // Send the Alarm SMS 
                            bool smsSuccess = false;


                            //=======================================================
                            // bool smsSuccess = gnaT.sendSMSArray(SMSmessage, smsMobile);

                            try
                            {

                                smsSuccess = gnaT.sendSMSArray(
                                    strSMSmessage: SMSmessage,
                                    smsMobile: smsMobile);

                            }
                            catch (Exception ex)
                            {
                                Console.WriteLine(ex.ToString());

                                Exception? innerException = ex.InnerException;

                                while (innerException != null)
                                {
                                    Console.WriteLine("INNER EXCEPTION:");
                                    Console.WriteLine(innerException.ToString());

                                    innerException = innerException.InnerException;
                                }
                            }


                            Console.WriteLine($"{strTab1}{(smsSuccess ? "SMS sent" : "SMS failed")}");

                            strMessage = "";
                            if (smsSuccess == true)
                            {
                                strMessage = $"{strReportType} Alarm: SMS Alarm message sent";
                            }
                            else
                            {
                                strMessage = $"{strReportType} Alarm: SMS Alarm message failed";
                            }

                            string smsList = string.Join(",", smsMobile);
                            logFileMessage = strMessage + "(" + smsList + ")";
                            gnaT.updateSystemLogFile(strSystemLogsFolder, logFileMessage);

                        }
                        else
                        {
                            Console.WriteLine($"{strTab1}No alarms detected");
                        }
                        Console.WriteLine($"{strTab1}Done");
                        #endregion


                 #region Send email if due


                        bool blnShouldSend = gnaT.ShouldTransmitEmail(emailCredentials: emailCreds);

                        if (blnShouldSend)
                        {
                            Console.WriteLine($"{strTab2}Email is due for transmission.");

                            #region Check time block type

                            string strNormalisedTimeBlockType = strTimeBlockType.Trim();

                            bool blnTransmitForTimeBlock =
                                strNormalisedTimeBlockType.Equals(value: "Manual", comparisonType: StringComparison.OrdinalIgnoreCase) ||
                                strNormalisedTimeBlockType.Equals(value: "Schedule", comparisonType: StringComparison.OrdinalIgnoreCase);

                            #endregion

                            #region Send / Do not send

                            if (blnTransmitForTimeBlock)
                            {
                                #region Prepare email content

                                emailCreds.Subject = $"{strReportType} Report: {strProjectTitle} ({strReportTime})";

                                string strSubMessage = t4dapi.extractMinTrackGeometry(
                                        trackPairList: trackPairList,
                                        prismList: prismList);

                                string missingPrisms = t4dapi.ExtractMissingPrisms(prismList);

                                strSubMessage = strSubMessage + missingPrisms;

                                strMessage =
                                    $"\nThis is an automated {strReportType} report for contract {strContractTitle}.\nPlease do not reply to this email.";

                                strMessage = strMessage + strTriggerHeader+"\nPRISMS IN ALARM STATE"+ strAlarmMessage+ "\n";

                                strMessage = gnaT.addCopyright("Track Geometry Report", strMessage);

                                emailCreds.Body = strMessage;

                                emailCreds.Attachments = new List<string>
                            {
                                strExportFile
                            };

                                #endregion

                                #region Transmit email

                                string strTransmitResult = gnaT.TransmitEmail(emailCredentials: emailCreds);

                                Console.WriteLine($"{strTab2}{strTransmitResult}");

                                #endregion
                            }
                            else
                            {
                                #region Do not send for Historic

                                Console.WriteLine($"{strTab2}Email not sent because strTimeBlockType is '{strTimeBlockType}'.");

                                #endregion
                            }

                            #endregion
                        }
                        else
                        {
                            #region Email not due

                            Console.WriteLine($"{strTab2}Email is not due for transmission.");

                            #endregion
                        }









                        #endregion

                        Console.WriteLine($"{strTab1}Done\n");
                    }


                }













ThatsAllFolks:

                Console.WriteLine("\nTrack Geometry Report completed...\n\n");
                gnaT.freezeScreen(strFreezeScreen);
                Environment.Exit(0);

            }
            catch (Exception ex)
            {
                File.WriteAllText("fatal_crash.log", ex.ToString());
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
