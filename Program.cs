using System.Configuration;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Reflection.Metadata.Ecma335;

using databaseAPI;

using EASendMail;

using gnaDataClasses;

using GNA_CommercialLicenseValidator;

using GNAgeneraltools;

using GNAspreadsheettools;

using GNAsurveytools;

using T4Dlibrary;

using Microsoft.Data.SqlClient;

using OfficeOpenXml;

using Twilio.Rest.Api.V2010.Account;
using Twilio.Rest.Sync.V1.Service.SyncStream;
using Twilio.TwiML.Messaging;
using Twilio.TwiML.Voice;







namespace TrackGeometryReport
{
    class Program
    {
        static void Main()
        {
            // This is a generic and expanded version of the SPN010 track geometry reports
            // additional featureds are added to make it more user friendly.



            try
            {


#pragma warning disable CS0162
#pragma warning disable CS8600
#pragma warning disable CS8601
#pragma warning disable CS8604




                //================[Instantiate the classes]======================================

                #region Initial setup



                gnaTools gnaT = new();
                GNAsurveycalcs gnaSurvey = new();
                dbAPI gnaDBAPI = new();
                spreadsheetAPI gnaSpreadsheetAPI = new(db: gnaDBAPI);
                T4Dapi t4dapi = new();





                Console.Clear();
                int headingNo = 1;

                // Welcome message
                gnaT.WelcomeMessage($"SPN010TGR {BuildInfo.BuildDateString()}");
                #endregion

                #region Check Config file and license
                Console.WriteLine($"{headingNo++}. Checking license and config file");
                string strTab1 = "     ";
                string strTab2 = "        ";
                gnaT.VerifyLocalConfig();
                var config = ConfigurationManager.AppSettings;

                string licenseCode = config["LicenseCode"] ?? string.Empty;
                if (string.IsNullOrEmpty(licenseCode))
                {
                    Console.WriteLine($"{strTab1}License code is not set in the configuration file.");
                    return;
                }

                // GNA license for TrackGeometryReport software
                Console.WriteLine($"{strTab1}Validating the software license...");

                LicenseValidator.ValidateLicense("TrackGeometryReport", licenseCode);
                Console.WriteLine($"{strTab2}Validated");

                //==== Set the EPPlus license
                gnaT.epplusLicense();

                #endregion


                #region Variables
                Console.WriteLine($"{headingNo++}. Variables");
                //================[Console settings]======================================
                Console.OutputEncoding = System.Text.Encoding.Unicode;

                //================[Declare variables]=====================================

                String[] strRO1 = new String[50];
                String[] strWorksheetName = new String[50];
                //string[] strTrackWorksheets = new String[50];


                //================[Configuration variables]==================================================================

                string strDBconnection = ConfigurationManager.ConnectionStrings["DBconnectionString"].ConnectionString;


                string strFreezeScreen = config["freezeScreen"];
                string strStopAtAlarmMessage = config["stopAtAlarmMessage"];
                string strAlarmVersion = config["AlarmVersion"];
                string strDeleteMissingValues = config["DeleteMissingValues"];
                string strLatestValueOnly = config["LatestValueOnly"];
                string strRecordHistoricData = config["recordHistoricData"];

                string strProjectTitle = config["ProjectTitle"];
                string strContractTitle = config["ContractTitle"];
                string strReportType = config["ReportType"];
                string strReportSpec = config["ReportSpec"];

                string strExcelPath = config["ExcelPath"];
                string strExcelFile = config["ExcelFile"];
                string strCoordinateOrder = config["CoordinateOrder"];

                string strReferenceWorksheet = config["ReferenceWorksheet"];
                string strSurveyWorksheet = config["SurveyWorksheet"];
                string strCalibrationWorksheet = config["CalibrationWorksheet"];
                string strHistoricDhworksheet = config["HistoricDhworksheet"];
                string strHistoricTopworksheet = config["HistoricTopworksheet"];
                string strHistoricTwistworksheet = config["HistoricTwistworksheet"];
                string strAlarmsWorksheet = config["AlarmsWorksheet"];

                string strWorkbookPassword = config["WorkbookPassword"];

                string strIncludeHistoricTwist = config["includeHistoricTwist"];
                string strIncludeHistoricSettlement = config["includeHistoricSettlement"];
                string strIncludeHistoricTop = config["includeHistoricTop"];
                string strIncludeMissingTargets = config["includeMissingTargets"];

                string strSystemLogsFolder = config["SystemStatusFolder"];
                string strAlarmfolder = config["SystemAlarmFolder"];

                #region Track Definitions
                var strTrackWorksheets = new List<string>();
                // Add the reference worksheet as the first item
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

                string strFirstDataRow = config["FirstDataRow"];
                string strFirstOutputRow = config["FirstOutputRow"];
                string strFirstDataCol = config["FirstDataCol"];
                string strFirstTrackRow = config["FirstTrackRow"];

                string strTimeBlockType = config["TimeBlockType"];
                string strManualBlockStart = config["manualBlockStart"];
                string strManualBlockEnd = config["manualBlockEnd"];
                string strBlockSizeHrs = config["BlockSizeHrs"];


                string strTimeBlockStartLocal = "";
                string strTimeBlockEndLocal = "";
                string strTimeBlockStartUTC = "";
                string strTimeBlockEndUTC = "";
                string strEmailTime = "";
                string logFileMessage = "";

                string strTempString = "";

                string strSPN010alarms = config["SPN010alarmNotifications"];
                string strSMSTitle = config["SMSTitle"];

                int iRow = Convert.ToInt32(strFirstDataRow);
                int iReferenceFirstDataRow = Convert.ToInt32(strFirstDataRow);
                int iFirstOutputRow = Convert.ToInt32(strFirstOutputRow);
                int iCol = Convert.ToInt32(strFirstDataCol);
                int iFirstTrackRow = Convert.ToInt32(strFirstTrackRow);

                string strSendEmails = config["SendEmails"];
                string strEmailLogin = config["EmailLogin"];
                string strEmailPassword = config["EmailPassword"];
                string strEmailFrom = config["EmailFrom"];
                string strEmailRecipients = config["EmailRecipients"];


                string strMasterWorkbookFullPath = strExcelPath + strExcelFile;
                string[,] strSensorID = new string[5000, 2];
                string[,] strPointDeltas = new string[5000, 2];
                string strDateTime = "";
                string strMasterFile = "";
                string strWorkingFile = "";
                string strExportFile = "";

                List<string> smsMobile = new();
                string strMobileList = "";
                var allKeys = config.AllKeys;
                var recipientKeys = allKeys.Where(k => k != null && k.StartsWith("RecipientPhone"));

                foreach (string key1 in recipientKeys)
                {
                    string value = config[key1];
                    if (!string.IsNullOrWhiteSpace(value))
                    {
                        smsMobile.Add(value);
                        if (strMobileList != "") strMobileList += ",";
                        strMobileList += value;
                    }
                }
                Console.WriteLine($"{strTab1}Assigned");
                #endregion

                #region Environment check
                Console.WriteLine($"{headingNo++}. Check system environment");
                if (strFreezeScreen == "Yes")
                {
                    Console.WriteLine($"{strTab1}Check DB connection");
                    gnaDBAPI.testDBconnection(strDBconnection);
                    Console.WriteLine($"{strTab2}Done");

                    Console.WriteLine($"{strTab1}Check existence of workbook & worksheets");

                    Console.WriteLine($"{strTab2}Project: {strProjectTitle}");
                    Console.WriteLine($"{strTab2}Report type: {strReportSpec}");
                    Console.WriteLine($"{strTab2}Master workbook: {strExcelFile}");
                    gnaSpreadsheetAPI.checkWorksheetExists(strMasterWorkbookFullPath, strReferenceWorksheet);
                    gnaSpreadsheetAPI.checkWorksheetExists(strMasterWorkbookFullPath, strSurveyWorksheet);
                    gnaSpreadsheetAPI.checkWorksheetExists(strMasterWorkbookFullPath, strAlarmsWorksheet);

                    if (strIncludeHistoricSettlement == "Yes")
                    {
                        gnaSpreadsheetAPI.checkWorksheetExists(strMasterWorkbookFullPath, strHistoricDhworksheet);
                    }
                    if (strIncludeHistoricTop == "Yes")
                    {
                        gnaSpreadsheetAPI.checkWorksheetExists(strMasterWorkbookFullPath, strHistoricTopworksheet);
                    }


                    int i = 1;  // skip index 0 (reference worksheet)

                    while (i < strTrackWorksheets.Count)
                    {
                        string entry = strTrackWorksheets[i];
                        if (string.IsNullOrWhiteSpace(entry))
                            break;  // stop safely on empty/missing values

                        string strTrackWorksheet = entry.Trim();
                        gnaSpreadsheetAPI.checkWorksheetExists(strMasterWorkbookFullPath, strTrackWorksheet);
                        if (strIncludeHistoricTwist == "Yes")
                        {
                            gnaSpreadsheetAPI.checkWorksheetExists(
                                strMasterWorkbookFullPath,
                                strTrackWorksheet + "_HistoricTwist");
                        }
                        i++;
                    }
                    Console.WriteLine($"{strTab1}Done");
                }
                else
                {
                    Console.WriteLine($"{strTab1}Environment check skipped");
                }



                #endregion


                #region Timeblocks

                //==== Prepare the time block
                Console.WriteLine($"{headingNo++}. Timeblocks");
                switch (strTimeBlockType)
                {
                    case "Manual":
                        strTimeBlockStartLocal = strManualBlockStart;
                        strTimeBlockEndLocal = strManualBlockEnd;
                        strTimeBlockStartUTC = gnaT.convertLocalToUTC(strTimeBlockStartLocal);
                        strTimeBlockEndUTC = gnaT.convertLocalToUTC(strTimeBlockEndLocal);
                        strEmailTime = string.Concat(strTimeBlockEndLocal.Replace("'", ""), "m");
                        break;
                    case "Schedule":

                        //double dblStartTimeOffset = -1.0 * Convert.ToDouble(strTimeOffsetHrs);
                        double dblEndTimeOffset = -1.0 * Convert.ToDouble(strBlockSizeHrs);
                        strTimeBlockEndLocal = " '" + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + "' ";
                        strTimeBlockStartLocal = " '" + DateTime.Now.AddHours(dblEndTimeOffset).ToString("yyyy-MM-dd HH:mm:ss") + "' ";
                        strTimeBlockStartUTC = gnaT.convertLocalToUTC(strTimeBlockStartLocal);
                        strTimeBlockEndUTC = gnaT.convertLocalToUTC(strTimeBlockEndLocal);
                        break;
                    default:
                        Console.WriteLine("\nError in Timeblock Type");
                        Console.WriteLine(strTab1 + "Time block type: " + strTimeBlockType);
                        Console.WriteLine(strTab1 + "Must be Manual or Schedule");
                        Console.WriteLine("\nPress key to exit..."); Console.ReadKey();
                        goto ThatsAllFolks;
                        break;
                }

                strDateTime = DateTime.Now.ToString("yyyyMMdd_HHmm");
                string strDateTimeUTC = DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm");   //2022-07-26 13:45:15
                string strTimeStamp = strTimeBlockEndLocal + "\n(local)";

                Console.WriteLine($"{strTab1}Time block type: {strTimeBlockType}");
                Console.WriteLine($"{strTab2}{strTimeBlockStartLocal.Replace("'", "")} Local");
                Console.WriteLine($"{strTab2}{strTimeBlockEndLocal.Replace("'", "")} Local");
                string strTimeStampLocal = "";

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


                #region Prepare Deltas
                Console.WriteLine($"{headingNo++}. Prepare deltas");
                //==== Process data ===================================================================================
                Console.WriteLine($"{strTab1}Extract point names");
                string[] strPointNames = gnaSpreadsheetAPI.readPointNames(strMasterFile, strSurveyWorksheet, strFirstDataRow);
                Console.WriteLine($"{strTab2}Done");
                Console.WriteLine($"{strTab1}Extract SensorID");
                strSensorID = gnaDBAPI.getSensorIDfromDB(strDBconnection, strPointNames, strProjectTitle);
                Console.WriteLine($"{strTab2}Done");
                Console.WriteLine($"{strTab1}Write SensorID to workbook");
                gnaSpreadsheetAPI.writeSensorID(strMasterFile, strSurveyWorksheet, strSensorID, strFirstDataRow);
                Console.WriteLine($"{strTab2}Done");

                if (strLatestValueOnly == "Yes")
                {
                    Console.WriteLine($"{strTab1}Extract latest deltas for time block");
                    strPointDeltas = gnaDBAPI.getLatestDeltasFromDB(strDBconnection, strProjectTitle, strTimeBlockStartUTC, strTimeBlockEndUTC, strSensorID);
                    strTempString = "latest";
                    Console.WriteLine($"{strTab2}Done");

                }
                else
                {
                    Console.WriteLine($"{strTab1}Extract mean deltas for UTC time block");
                    Console.WriteLine($"{strTab2}{strTimeBlockStartUTC.Replace("'", "")}");
                    Console.WriteLine($"{strTab2}{strTimeBlockEndUTC.Replace("'", "")}");
                    strPointDeltas = gnaDBAPI.getMeanDeltasFromDB(strDBconnection, strProjectTitle, strTimeBlockStartUTC, strTimeBlockEndUTC, strSensorID);
                    strTempString = "mean";
                    Console.WriteLine($"{strTab2}Done");
                }

                Console.WriteLine($"{strTab1}Write {strTempString} deltas & timestamp to master workbook");
                string strBlockStart = strTimeBlockStartUTC.Replace("'", "").Trim();
                string strBlockEnd = strTimeBlockEndUTC.Replace("'", "").Trim();

                gnaSpreadsheetAPI.writeLatestDeltas(
                    strMasterFile,
                    strReferenceWorksheet,
                    strPointDeltas,
                    iRow, iCol, strBlockStart,
                    strBlockEnd,
                    strCoordinateOrder);

                gnaSpreadsheetAPI.writeTimeStampLocal(
                    strMasterFile,
                    strReferenceWorksheet,
                    strTimeStampLocal);
                Console.WriteLine($"{strTab2}Done");
                #endregion


                #region Write historic data
                Console.WriteLine($"{headingNo++}. Write historic data");

                if (strAlarmVersion == "Yes")
                {
                    Console.WriteLine($"{strTab1}Alarm version activated - skipping historic data update.");
                    goto CalibrationData;
                }
                else if (strRecordHistoricData != "Yes")
                {
                    Console.WriteLine($"{strTab1}Historic data recording not activated - skipping historic data update.");
                    goto CalibrationData;
                }

                Console.WriteLine($"{strTab1}Write historic twist");

                // write the historic twist data if applicable
                if (!string.IsNullOrWhiteSpace(strIncludeHistoricTwist) &&
                    strIncludeHistoricTwist.Trim().Equals("Yes", StringComparison.OrdinalIgnoreCase))
                {


                    if (strTrackWorksheets == null || strTrackWorksheets.Count <= 1)
                    {
                        Console.WriteLine($"{strTab2}No valid track worksheets supplied.");
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
                            string strHistoricTwistWorksheet = strTrackWorksheet + "_HistoricTwist";

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


                                // Find the last data row in the Historic Twist worksheet
                                int iNoOfPrisms = gnaSpreadsheetAPI.countPrisms(strMasterFile, strHistoricTwistWorksheet, "8", 1);
                                int iRowEnd = 8 + iNoOfPrisms;


                                try
                                {
                                    // Copy the header cells
                                    gnaSpreadsheetAPI.copyColumnSubRange(
                                        strMasterFile,
                                        strHistoricTwistWorksheet,  // source worksheet
                                        3,                      // source column
                                        strHistoricTwistWorksheet, // destination worksheet
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
                                    iSourceCol = 12;     // Column AW in the reference worksheet (dH in mm);
                                    int iSourceRowStart = 8;   // Row 2 in the reference worksheet
                                    int iSourceRowEnd = iRowEnd;  // Last row in the source worksheet containing rail prisms.
                                    int iDestinationRowStart = 8; // Row 8 in the historic dH worksheet
                                    iDestinationCol = iFirstEmptyCol;

                                    try
                                    {
                                        // Copy the data cells
                                        gnaSpreadsheetAPI.copyColumnSubRange(
                                            strMasterFile,
                                            strTrackWorksheet,      // source worksheet
                                            iSourceCol,             // source column
                                            strHistoricTwistWorksheet, // destination worksheet
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

                    // Find first empty column in the Historic data worksheet
                    int iFirstEmptyCol = gnaSpreadsheetAPI.findFirstEmptyColumn(
                        strMasterFile,
                        strHistoricDhworksheet,
                        "6",
                        "1");


                    // Find the last data row in the Historic data worksheet
                    int iNoOfPrisms = gnaSpreadsheetAPI.countPrisms(strMasterFile, strHistoricDhworksheet, "8", 1);
                    int iRowEnd = 8 + iNoOfPrisms;


                    // Copy the header cells
                    gnaSpreadsheetAPI.copyColumnSubRange(
                        strMasterFile,
                        strHistoricDhworksheet,  // source worksheet
                        3,                      // source column
                        strHistoricDhworksheet, // destination worksheet
                        iFirstEmptyCol,         // destination column
                        6,                      // source start row
                        7,                      // source end end
                        6                       // destination start row
                     );


                    // Insert the timestamp
                    gnaSpreadsheetAPI.writeVarToCell(
                        strMasterFile,
                        strHistoricDhworksheet,
                        5,
                        iFirstEmptyCol,
                        strHeaderTime);


                    // Insert data range
                    if (iFirstEmptyCol <= 1)
                    {
                        Console.WriteLine($"{strTab1}WARN: Invalid column index ({iFirstEmptyCol}) for '{strHistoricDhworksheet}'. Skipping.");
                    }
                    else
                    {
                        int iSourceCol = 49;     // Column AW in the reference worksheet (dH in mm);
                        int iSourceRowStart = 2;   // Row 2 in the reference worksheet
                        int iSourceRowEnd = iSourceRowStart + iNoOfPrisms - 1;  // Last row in the reference worksheet containing rail prisms.
                        int iDestinationRowStart = 8; // Row 8 in the historic dH worksheet
                        int iDestinationCol = iFirstEmptyCol;

                        try
                        {
                            // Copy the data cells
                            gnaSpreadsheetAPI.copyColumnSubRange(
                                strMasterFile,
                                strReferenceWorksheet,  // source worksheet
                                iSourceCol,             // source column
                                strHistoricDhworksheet, // destination worksheet
                                iDestinationCol,        // destination column
                                iSourceRowStart,        // source start row
                                iSourceRowEnd,          // source end row
                                iDestinationRowStart    // destination start row
                             );
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine(strTab1 + $"ERROR copying from '{strReferenceWorksheet}' → '{strHistoricDhworksheet}\n': {ex.Message}");
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

                    // Find first empty column in the Historic data worksheet
                    int iFirstEmptyCol = gnaSpreadsheetAPI.findFirstEmptyColumn(
                        strMasterFile,
                        strHistoricTopworksheet,
                        "6",
                        "1");


                    // Find the last data row in the Historic data worksheet
                    int iNoOfPrisms = gnaSpreadsheetAPI.countPrisms(strMasterFile, strHistoricTopworksheet, "8", 1);
                    int iRowEnd = 8 + iNoOfPrisms;

                    // Copy the header cells
                    gnaSpreadsheetAPI.copyColumnSubRange(
                        strMasterFile,
                        strHistoricTopworksheet,  // source worksheet
                        3,                      // source column
                        strHistoricTopworksheet, // destination worksheet
                        iFirstEmptyCol,         // destination column
                        6,                      // source start row
                        7,                      // source end end
                        6                       // destination start row
                     );


                    // Insert the timestamp
                    gnaSpreadsheetAPI.writeVarToCell(
                        strMasterFile,
                        strHistoricTopworksheet,
                        5,
                        iFirstEmptyCol,
                        strHeaderTime);


                    // Insert data range
                    if (iFirstEmptyCol <= 1)
                    {
                        Console.WriteLine($"{strTab2}WARN: Invalid column index ({iFirstEmptyCol}) for '{strHistoricTopworksheet}'. Skipping.");
                    }
                    else
                    {
                        int iSourceCol = 50;     // Column AX in the reference worksheet (Top in mm);
                        int iSourceRowStart = 2;   // Row 2 in the reference worksheet
                        int iSourceRowEnd = iSourceRowStart + iNoOfPrisms - 1;  // Last row in the reference worksheet containing rail prisms.
                        int iDestinationRowStart = 8; // Row 8 in the historic dH worksheet
                        int iDestinationCol = iFirstEmptyCol;
                        try
                        {
                            // Copy the data cells
                            gnaSpreadsheetAPI.copyColumnSubRange(
                                strMasterFile,
                                strReferenceWorksheet,  // source worksheet
                                iSourceCol,             // source column
                                strHistoricTopworksheet, // destination worksheet
                                iDestinationCol,        // destination column
                                iSourceRowStart,        // source start row
                                iSourceRowEnd,          // source end row
                                iDestinationRowStart    // destination start row
                             );
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"{strTab2}ERROR copying from '{strReferenceWorksheet}' → '{strHistoricTopworksheet}\n': {ex.Message}");
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


CalibrationData:

#region Calibration data

                Console.WriteLine($"{headingNo++}. Calibration data");
                Console.WriteLine($"{strTab1}Skip this section");
                //string strDistanceColumn = "3";
                //gnaSpreadsheetAPI.populateCalibrationWorksheet(strDBconnection, strTimeBlockStartUTC, strTimeBlockEndUTC, strWorkingFile, strCalibrationWorksheet, strFirstOutputRow, strDistanceColumn, strProjectTitle);

                #endregion

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

                // Start at 1 to skip element 0 (reference worksheet)
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
    }
}
