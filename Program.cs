using System.Collections.Specialized;
using System.Configuration;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
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

                // Set the T4DAPI license: Software license expires 20260601
                string T4DAPI_licenseCode = "Dm4eGwoTaGxqbGpr";
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
                string? strHistoricTwistWorksheet = config["HistoricTwistWorksheet"]?.Trim();
                string? strAlarmsWorksheet = config["AlarmsWorksheet"]?.Trim();
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

                #region Track Variables
                Console.WriteLine($"{strTab1}Track variables");
                string strFirstDataCol = config["FirstDataCol"];
                string strFirstTrackRow = config["FirstTrackRow"];

                var strTrackWorksheets = new List<string>();
                // Add the reference Worksheet as the first item
                strTrackWorksheets.Add(strReferenceWorksheet);

                // Read all configured tracks dynamically (Track1, Track2, ..., TrackN)
                foreach (var key0 in ConfigurationManager.AppSettings.AllKeys)
                {
                    if (key0.StartsWith("Track", StringComparison.OrdinalIgnoreCase))
                    {
                        var value = ConfigurationManager.AppSettings[key0]?.Trim();
                        if (!string.IsNullOrEmpty(value))
                            strTrackWorksheets.Add(value);
                    }
                }
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
                int iCol = Convert.ToInt32(strFirstDataCol);
                int iFirstTrackRow = Convert.ToInt32(strFirstTrackRow);

                Console.WriteLine($"{strTab1}Assigned");
                #endregion

                #region populate the RuntimeEnvironment class

                gnaDataClasses.RuntimeEnvironment runtimeEnvironment = new()
                {
                    // ---- Database ----
                    DbConnectionString = strDBconnection,
                    ProjectTitle = strProjectTitle,

                    // ---- Workbook ----
                    ExcelPath = strExcelPath,
                    ExcelFile = strExcelFile,

                    // ---- Worksheets ----
                    ReferenceWorksheet = strReferenceWorksheet,
                    SurveyWorksheet = strSurveyWorksheet,
                    TrackGeometryWorksheet = strTrackGeometryWorksheet,
                    HistoricTopWorksheet = strHistoricTopWorksheet,
                    HistoricTwistWorksheet = strHistoricTwistWorksheet,

                    // ---- Row/Col configuration ----
                    FirstDataRow = iFirstDataRow,
                    FirstDataCol = iFirstDataCol,
                    FirstOutputRow = iFirstOutputRow
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
                    gnaSpreadsheetAPI.checkWorksheetExists(strMasterWorkbookFullPath, strHistoricTopWorksheet);
                    gnaSpreadsheetAPI.checkWorksheetExists(strMasterWorkbookFullPath, strHistoricTwistWorksheet);
                    gnaSpreadsheetAPI.checkWorksheetExists(strMasterWorkbookFullPath, strAlarmsWorksheet);
                    Console.WriteLine($"{strTab1}Done");
                }
                else
                {
                    Console.WriteLine($"{strTab1}Environment check skipped");
                }



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
                            "Historic",
                            strBlockSizeHrs,
                            strManualBlockStart,
                            strManualBlockEnd,
                            dblTimeZoneOffset);
                        break;

                    case "Manual":
                        subBlocks = gnaT.prepareTimeBlocksWithTimeZoneOffset(
                            "Manual",
                            strManualBlockStart,
                            strManualBlockEnd,
                            dblTimeZoneOffset);
                        break;

                    case "Schedule":
                        subBlocks = gnaT.prepareTimeBlocksWithTimeZoneOffset(
                            "Schedule",
                            strBlockSizeHrs,
                            strManualBlockStart,
                            strManualBlockEnd,
                            dblTimeZoneOffset);
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

                if (freezeScreen)
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
                    Console.WriteLine($"{strTab1}No{strSurveyWorksheet} preparation ");
                }
                #endregion

                #region Create prism sensor list
                Console.WriteLine($"{headingNo++}. Create sensor list: Prisms");

                List<Sensor> sensorsList = gnaSpreadsheetAPI.readSensors(
                    runtimeEnvironment: runtimeEnvironment);

                SensorType sensorTypeList = t4dapi.DetermineAvailableSensorTypes(
                    sensorsList: sensorsList);

                List<Sensor> prismSensorsList = sensorsList
                    .Where(sensor => sensor.SensorID != "Missing" && sensor.SensorType == "Prism")
                    .ToList();

                List<Points> prismList = t4dapi.GetSensorList(strDBconnection, strProjectTitle);

                Console.WriteLine($"{strTab1}Prism list: {prismSensorsList.Count.ToString(CultureInfo.InvariantCulture)}");
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
                    string result = t4dapi.writeDeltasToReferenceWorksheet(
                        prismList: prismList,
                        blockStartUTC: blockStartUTC,
                        blockEndUTC: blockEndUTC,
                        runtimeEnvironment: runtimeEnvironment);
                    Console.WriteLine($"{strTab1}{result}");


                    Console.WriteLine("\nStop here");
                    Console.ReadKey();

                    #endregion










                    Console.WriteLine($"{strTab2}Prism");



                    Console.WriteLine($"{strTab1}Reference deltas written to {strSurveyWorksheet} worksheet");


                    //goto ThatsAllFolks;


                }
                else
                {
                    Console.WriteLine($"{strTab1}No reference deltas generated.");
                }

                #endregion





















                #region Write historic data
                Console.WriteLine($"{headingNo++}. Write historic data");

                if (strAlarmVersion == "Yes")
                {
                    Console.WriteLine($"{strTab1}Alarm version activated - skipping historic data update.");
                    goto EntryPoint1;
                }
                else if (strRecordHistoricData != "Yes")
                {
                    Console.WriteLine($"{strTab1}Historic data recording not activated - skipping historic data update.");
                    goto EntryPoint1;
                }

                Console.WriteLine($"{strTab1}Write historic twist");

                // write the historic twist data if applicable
                if (!string.IsNullOrWhiteSpace(strIncludeHistoricTwist) &&
                    strIncludeHistoricTwist.Trim().Equals("Yes", StringComparison.OrdinalIgnoreCase))
                {


                    if (strTrackWorksheets == null || strTrackWorksheets.Count <= 1)
                    {
                        Console.WriteLine($"{strTab2}No valid track Worksheets supplied.");
                    }
                    else
                    {
                        int i = 1; // 1-based indexing retained
                        string strHeaderTime = strTimeBlockEndLocal.Replace("'", "").Trim();

                        while (i < strTrackWorksheets.Count)
                        {
                            string? entry = strTrackWorksheets[i];

                            // Guard null or empty elements
                            if (string.IsNullOrWhiteSpace(entry))
                            {
                                Console.WriteLine($"{strTab2}Null or empty entry at index {i}. Terminating.");
                                break;
                            }

                            string trimmed = entry.Trim();

                            string strTrackWorksheet = trimmed;


                            Console.WriteLine(strTab2 + strHistoricTwistWorksheet);

                            // Defensive: ensure API call returns a positive column index
                            int iFirstEmptyCol = gnaSpreadsheetAPI.findFirstEmptyColumn(
                                strMasterFile,
                                strHistoricTwistWorksheet,
                                "6",
                                "1");

                            if (iFirstEmptyCol <= 1)
                            {
                                Console.WriteLine($"{strTab1}WARN: Invalid column index ({iFirstEmptyCol}) for '{strHistoricTwistWorksheet}'. Skipping.");
                            }
                            else
                            {
                                int iSourceCol = 12;
                                int iDestinationCol = iFirstEmptyCol;


                                // Find the last data row in the Historic Twist Worksheet
                                int iNoOfPrisms = gnaSpreadsheetAPI.countPrisms(strMasterFile, strHistoricTwistWorksheet, "8", 1);
                                int iRowEnd = 8 + iNoOfPrisms;


                                try
                                {
                                    // Copy the header cells
                                    gnaSpreadsheetAPI.copyColumnSubRange(
                                        strMasterFile,
                                        strHistoricTwistWorksheet,  // source Worksheet
                                        3,                      // source column
                                        strHistoricTwistWorksheet, // destination Worksheet
                                        iFirstEmptyCol,         // destination column
                                        6,                      // source start row
                                        7,                      // source end end
                                        6                       // destination start row
                                     );


                                    // Insert the timestamp
                                    gnaSpreadsheetAPI.writeVarToCell(
                                        strMasterFile,
                                        strHistoricTwistWorksheet,
                                        5,
                                        iFirstEmptyCol,
                                        strHeaderTime);


                                    // Insert the data range
                                    iSourceCol = 12;     // Column AW in the reference Worksheet (dH in mm);
                                    int iSourceRowStart = 8;   // Row 2 in the reference Worksheet
                                    int iSourceRowEnd = iRowEnd;  // Last row in the source Worksheet containing rail prisms.
                                    int iDestinationRowStart = 8; // Row 8 in the historic dH Worksheet
                                    iDestinationCol = iFirstEmptyCol;

                                    try
                                    {
                                        // Copy the data cells
                                        gnaSpreadsheetAPI.copyColumnSubRange(
                                            strMasterFile,
                                            strTrackWorksheet,      // source Worksheet
                                            iSourceCol,             // source column
                                            strHistoricTwistWorksheet, // destination Worksheet
                                            iDestinationCol,        // destination column
                                            iSourceRowStart,        // source start row
                                            iSourceRowEnd,          // source end row
                                            iDestinationRowStart    // destination start row
                                         );

                                    }
                                    catch (Exception ex)
                                    {
                                        Console.WriteLine($"{strTab1}ERROR copying from '{strTrackWorksheet}' → '{strHistoricTwistWorksheet}\n': {ex.Message}");
                                        Console.ReadKey();
                                    }

                                }
                                catch (Exception ex)
                                {
                                    Console.WriteLine($"{strTab1}ERROR copying from '{strTrackWorksheet}' → '{strHistoricTwistWorksheet}\n': {ex.Message}");
                                }
                            }
                            i++;
                        }

                    }
                }
                else
                {
                    Console.WriteLine($"{strTab2}Not activated");
                }


                Console.WriteLine($"{strTab1}Write historic dH");

                // write the historic dH data if applicable
                if (!string.IsNullOrWhiteSpace(strIncludeHistoricSettlement) &&
                    strIncludeHistoricSettlement.Trim().Equals("Yes", StringComparison.OrdinalIgnoreCase))
                {
                    Console.WriteLine($"{strTab2}Activated");

                    string strHeaderTime = strTimeBlockEndLocal.Replace("'", "").Trim();

                    // Find first empty column in the Historic data Worksheet
                    int iFirstEmptyCol = gnaSpreadsheetAPI.findFirstEmptyColumn(
                        strMasterFile,
                        strHistoricDhWorksheet,
                        "6",
                        "1");


                    // Find the last data row in the Historic data Worksheet
                    int iNoOfPrisms = gnaSpreadsheetAPI.countPrisms(strMasterFile, strHistoricDhWorksheet, "8", 1);
                    int iRowEnd = 8 + iNoOfPrisms;


                    // Copy the header cells
                    gnaSpreadsheetAPI.copyColumnSubRange(
                        strMasterFile,
                        strHistoricDhWorksheet,  // source Worksheet
                        3,                      // source column
                        strHistoricDhWorksheet, // destination Worksheet
                        iFirstEmptyCol,         // destination column
                        6,                      // source start row
                        7,                      // source end end
                        6                       // destination start row
                     );


                    // Insert the timestamp
                    gnaSpreadsheetAPI.writeVarToCell(
                        strMasterFile,
                        strHistoricDhWorksheet,
                        5,
                        iFirstEmptyCol,
                        strHeaderTime);


                    // Insert data range
                    if (iFirstEmptyCol <= 1)
                    {
                        Console.WriteLine($"{strTab1}WARN: Invalid column index ({iFirstEmptyCol}) for '{strHistoricDhWorksheet}'. Skipping.");
                    }
                    else
                    {
                        int iSourceCol = 49;     // Column AW in the reference Worksheet (dH in mm);
                        int iSourceRowStart = 2;   // Row 2 in the reference Worksheet
                        int iSourceRowEnd = iSourceRowStart + iNoOfPrisms - 1;  // Last row in the reference Worksheet containing rail prisms.
                        int iDestinationRowStart = 8; // Row 8 in the historic dH Worksheet
                        int iDestinationCol = iFirstEmptyCol;

                        try
                        {
                            // Copy the data cells
                            gnaSpreadsheetAPI.copyColumnSubRange(
                                strMasterFile,
                                strReferenceWorksheet,  // source Worksheet
                                iSourceCol,             // source column
                                strHistoricDhWorksheet, // destination Worksheet
                                iDestinationCol,        // destination column
                                iSourceRowStart,        // source start row
                                iSourceRowEnd,          // source end row
                                iDestinationRowStart    // destination start row
                             );
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine(strTab1 + $"ERROR copying from '{strReferenceWorksheet}' → '{strHistoricDhWorksheet}\n': {ex.Message}");
                            Console.ReadKey();
                        }

                    }
                }
                else
                {
                    Console.WriteLine($"{strTab2}Not activated");
                }


                // write the historic Top if applicable
                Console.WriteLine($"{strTab1}Write historic Top");

                // write the historic Top data if applicable
                if (!string.IsNullOrWhiteSpace(strIncludeHistoricTop) &&
                    strIncludeHistoricTop.Trim().Equals("Yes", StringComparison.OrdinalIgnoreCase))
                {
                    Console.WriteLine(strTab2 + "Activated");

                    string strHeaderTime = strTimeBlockEndLocal.Replace("'", "").Trim();

                    // Find first empty column in the Historic data Worksheet
                    int iFirstEmptyCol = gnaSpreadsheetAPI.findFirstEmptyColumn(
                        strMasterFile,
                        strHistoricTopWorksheet,
                        "6",
                        "1");


                    // Find the last data row in the Historic data Worksheet
                    int iNoOfPrisms = gnaSpreadsheetAPI.countPrisms(strMasterFile, strHistoricTopWorksheet, "8", 1);
                    int iRowEnd = 8 + iNoOfPrisms;

                    // Copy the header cells
                    gnaSpreadsheetAPI.copyColumnSubRange(
                        strMasterFile,
                        strHistoricTopWorksheet,  // source Worksheet
                        3,                      // source column
                        strHistoricTopWorksheet, // destination Worksheet
                        iFirstEmptyCol,         // destination column
                        6,                      // source start row
                        7,                      // source end end
                        6                       // destination start row
                     );


                    // Insert the timestamp
                    gnaSpreadsheetAPI.writeVarToCell(
                        strMasterFile,
                        strHistoricTopWorksheet,
                        5,
                        iFirstEmptyCol,
                        strHeaderTime);


                    // Insert data range
                    if (iFirstEmptyCol <= 1)
                    {
                        Console.WriteLine($"{strTab2}WARN: Invalid column index ({iFirstEmptyCol}) for '{strHistoricTopWorksheet}'. Skipping.");
                    }
                    else
                    {
                        int iSourceCol = 50;     // Column AX in the reference Worksheet (Top in mm);
                        int iSourceRowStart = 2;   // Row 2 in the reference Worksheet
                        int iSourceRowEnd = iSourceRowStart + iNoOfPrisms - 1;  // Last row in the reference Worksheet containing rail prisms.
                        int iDestinationRowStart = 8; // Row 8 in the historic dH Worksheet
                        int iDestinationCol = iFirstEmptyCol;
                        try
                        {
                            // Copy the data cells
                            gnaSpreadsheetAPI.copyColumnSubRange(
                                strMasterFile,
                                strReferenceWorksheet,  // source Worksheet
                                iSourceCol,             // source column
                                strHistoricTopWorksheet, // destination Worksheet
                                iDestinationCol,        // destination column
                                iSourceRowStart,        // source start row
                                iSourceRowEnd,          // source end row
                                iDestinationRowStart    // destination start row
                             );
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"{strTab2}ERROR copying from '{strReferenceWorksheet}' → '{strHistoricTopWorksheet}\n': {ex.Message}");
                            Console.ReadKey();
                        }

                    }
                }
                else
                {
                    Console.WriteLine($"{strTab2}Not activated");
                }

                Console.WriteLine($"{strTab1}Done");
                #endregion

EntryPoint1:
#region Top,twist, missing targets alarms
                Console.WriteLine($"{headingNo++}. Top,Twist,Long Twist, missing targets alarm state & SMS if alarms");

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
                        Console.WriteLine($"{strAlarmMessage}\n\n"); // multiline causes odd output alignment in console
                    }

                    string SMSmessage = strSMSTitle + ":" + strTimeNow + "\n" + strAlarmMessage;

                    // Send the Alarm SMS 

                    bool smsSuccess = gnaT.sendSMSArray(SMSmessage, smsMobile);
                    Console.WriteLine($"{strTab1}{(smsSuccess ? "SMS sent" : "SMS failed")}");
                    strMessage = "";
                    if (smsSuccess == true)
                    {
                        strMessage = "TrackGeometryReport Alarm: SMS Alarm message sent";
                    }
                    else
                    {
                        strMessage = "TrackGeometryReport Alarm: SMS Alarm message failed";
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


                #region Prepare the export Report

                //ExportWorkbook:

                Console.WriteLine($"{headingNo++}. Create the export workbook");

                // Update the time stamp in case the software was put on hold waiting for a specific time..
                strDateTime = DateTime.Now.ToString("yyyyMMdd_HHmm");
                strExportFile = strExportFile.Replace("DateTime", strDateTime);


                gnaSpreadsheetAPI.copyWorkbook(strMasterFile, strExportFile);
                Console.WriteLine($"{strTab1}{strExportFile}");
                Console.WriteLine($"{strTab1}Done");

                Console.WriteLine($"{strTab1}Clean export workbook to match TrackGeometryReport template");

                // Start at 1 to skip element 0 (reference Worksheet)
                for (int j = 1; j < strTrackWorksheets.Count; j++)
                {
                    string strTrackWorksheet = strTrackWorksheets[j].Trim();

                    if (string.IsNullOrWhiteSpace(strTrackWorksheet))
                        continue;   // or break; depending on how strict you want to be

                    Console.WriteLine($"{strTab1}{strTrackWorksheet}");

                    // convert Columns 2 & 6 to numbers
                    Console.WriteLine($"{strTab2}Convert references to values");
                    gnaSpreadsheetAPI.convertWorksheetFormulae(strExportFile, strTrackWorksheet, iFirstOutputRow, 2, 2);    // Left rail reduced level at target
                    gnaSpreadsheetAPI.convertWorksheetFormulae(strExportFile, strTrackWorksheet, iFirstOutputRow, 6, 6);    // Right rail prism ht

                    if (strDeleteMissingValues == "Yes")
                    {
                        Console.WriteLine($"{strTab1}Delete missing data");
                        gnaSpreadsheetAPI.removeSPN010missingData(strExportFile, strTrackWorksheet);
                        Console.WriteLine($"{strTab2}Done");
                    }
                    else
                    {
                        Console.WriteLine($"{strTab2}Missing data not deleted");
                    }
                }

                Console.WriteLine($"{strTab1}Freeze the export workbook");
                Console.WriteLine($"{strTab2}Hide {strReferenceWorksheet}");
                gnaSpreadsheetAPI.hideWorksheet(strExportFile, strReferenceWorksheet);
                Console.WriteLine($"{strTab2}Hide {strAlarmsWorksheet}");
                gnaSpreadsheetAPI.hideWorksheet(strExportFile, strAlarmsWorksheet);
                Console.WriteLine($"{strTab2}Hide {strSurveyWorksheet}");
                gnaSpreadsheetAPI.hideWorksheet(strExportFile, strSurveyWorksheet);
                Console.WriteLine($"{strTab2}Freeze {strExportFile}");
                gnaSpreadsheetAPI.freezeWorkbook(strExportFile, strWorkbookPassword);
                Console.WriteLine($"{strTab1}Done");
                #endregion

                #region Send the export Report

                Console.WriteLine($"{headingNo++}. email the export workbook");

                if (strSendEmails == "Yes")
                {

                    try
                    {
                        strMessage = null;
                        if (strAlarmMessage != "No Alarm")
                        {
                            string strSPN010TriggerHeader =
    "TrackGeometryReport Trigger Criteria\n" +
    "\n" +
    "LIMITING CRITERIA FOR SHORT TWIST (3m)\n" +
    "Twist < 1 in 500: 500\n" +
    "Twist between 1 in 500 and 1 in 250: 250\n" +
    "Twist > 1 in 250: 0\n" +
    "\n" +
    "LIMITING CRITERIA FOR LONG TWIST (15m)\n" +
    "Warp < 1 in 800: 800\n" +
    "Warp between 1 in 400 and 1 in 800: 400\n" +
    "Warp > 1 in 400: 0\n" +
    "\n" +
    "LIMITING CRITERIA FOR TOP\n" +
    "Top < 7.5 over 6m: 0\n" +
    "Top between 7.5 and 10: 7.5\n" +
    "Top over 10mm: 10";

                            strMessage = "This is an automated " + strReportSpec + " track geometry report.\n\nCurrent Project State:\n" + strAlarmMessage + "\n\n" +
                              strSPN010TriggerHeader +
                              "\n\nPlease review and forward to the client. \nDo not reply to this email.";
                        }
                        else
                        {
                            strMessage = "This is an automated " + strReportSpec + " track geometry report.\n\nMissing prisms: None\nCurrent Project State: Top,Twist,Long Twist OK\n\nDo not reply to this email.";
                        }

                        strMessage = gnaT.addCopyright("TrackGeometryReport", strMessage);


                        // updated with the 20240816 license
                        string license = gnaT.commercialSoftwareLicense("email");
                        SmtpMail oMailEmail = new(license)
                        {
                            //Set sender email address
                            From = strEmailFrom,
                            To = new AddressCollection(strEmailRecipients),
                            Subject = "TrackGeometryReport: " + strProjectTitle + " (" + strDateTime + ")",
                            TextBody = strMessage
                        };
                        oMailEmail.AddAttachment(strExportFile);
                        // SMTP server address
                        SmtpServer oServerEmail = new("smtp.gmail.com")
                        {
                            User = strEmailLogin,
                            Password = strEmailPassword,
                            ConnectType = SmtpConnectType.ConnectTryTLS,
                            Port = 587
                        };

                        //Set sender email address, please change it to yours
                        SmtpClient oSmtpEmail = new();
                        oSmtpEmail.SendMail(oServerEmail, oMailEmail);
                        strMessage = strReportSpec + " Track Geometry Report: " + strProjectTitle + " (" + strDateTime + ")" + " (emailed:";
                        logFileMessage = strMessage + strEmailRecipients + ")";
                        gnaT.updateSystemLogFile(strSystemLogsFolder, logFileMessage);
                        gnaT.updateReportTime("TrackGeometryReport");

                        Console.WriteLine($"{strTab1}Done");

                    }
                    catch (Exception ep)
                    {
                        Console.WriteLine("Failed to send email with the following error:");
                        Console.WriteLine(strEmailLogin);
                        Console.WriteLine(strEmailPassword);
                        Console.WriteLine(ep.Message);
                        Console.ReadKey();
                    }
                }
                else
                {
                    Console.WriteLine($"{strTab1}No email sent");
                }

#endregion

ThatsAllFolks:

                Console.WriteLine("\nSPN010 report completed...\n\n");
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
