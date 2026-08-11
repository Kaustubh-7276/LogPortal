using BancsEventsLogger.Models;
using Oracle.ManagedDataAccess.Client;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Web.Mvc;

namespace BancsEventsLogger.Controllers
{
    public class MakerCheckerController : Controller
    {
        private readonly string connString = ConfigurationManager.ConnectionStrings["dbconnectionv3"].ConnectionString;

        // GET: MakerChecker
        public ActionResult Index()
        {
            List<TransactionModel> transactions = new List<TransactionModel>();
            try
            {
                using (OracleConnection conn = new OracleConnection(connString))
                {
                    // Selecting the specific columns requested for the header
                    string sql = @"SELECT TXNNO, CAPABILITY, TITLE, ISFINANCIAL, 
                                   NUMCHCKRS, CHECKINGREQD, CPC_NUMCHCKRS, CPC_CHECKINGREQD 
                                   FROM transactions ORDER BY TXNNO DESC";

                    OracleCommand cmd = new OracleCommand(sql, conn);
                    conn.Open();
                    using (OracleDataReader dr = cmd.ExecuteReader())
                    {
                        while (dr.Read())
                        {
                            transactions.Add(new TransactionModel
                            {
                                TXNNO = Convert.ToInt32(dr["TXNNO"]),
                                CAPABILITY = dr["CAPABILITY"]?.ToString(),
                                TITLE = dr["TITLE"]?.ToString(),
                                ISFINANCIAL = dr["ISFINANCIAL"]?.ToString(),
                                NUMCHCKRS = dr["NUMCHCKRS"]?.ToString(),
                                CHECKINGREQD = dr["CHECKINGREQD"]?.ToString(),
                                CPC_NUMCHCKRS = dr["CPC_NUMCHCKRS"]?.ToString(),
                                CPC_CHECKINGREQD = dr["CPC_CHECKINGREQD"]?.ToString()
                            });
                        }
                    }
                }
            }
            catch (Exception ex) { TempData["Error"] = ex.Message; }
            return View(transactions);
        }

        [HttpPost]
        public ActionResult ProcessTransaction(int txnNo, string regionDetails, string actionType, string capability = null, string title = null, int? isFinancial = null)
        {
            // Configure numerical and string values for parameters
            int numChckrs = (actionType == "MARK") ? 1 : 0;
            string checkReq = (actionType == "MARK") ? "Y" : "N";
            string DBconnString;
            switch (regionDetails)
            {
                case "V3": case "DEV":
                    DBconnString = ConfigurationManager.ConnectionStrings["dbconnectionv3"].ConnectionString;
                    break;
                case "V4":
                    DBconnString = ConfigurationManager.ConnectionStrings["dbconnectionv4"].ConnectionString;
                    break;
                case "WD":
                    DBconnString = ConfigurationManager.ConnectionStrings["dbconnectionWD"].ConnectionString;
                    break;
                default:
                    DBconnString = ConfigurationManager.ConnectionStrings["dbconnectionv3"].ConnectionString;
                    break;
            }
            if (!String.IsNullOrWhiteSpace(DBconnString))
            {
                using (OracleConnection conn = new OracleConnection(DBconnString))
                {
                    try
                    {
                        conn.Open();

                        // 1. UPDATE QUERY
                        string updateSql = @"UPDATE transactions 
                                 SET NUMCHCKRS = :nc, CHECKINGREQD = :cr, 
                                     CPC_NUMCHCKRS = :nc, CPC_CHECKINGREQD = :cr 
                                 WHERE TXNNO = :id";

                        using (OracleCommand cmd = new OracleCommand(updateSql, conn))
                        {
                            cmd.BindByName = true;
                            // Set a short timeout (e.g., 5 seconds) to detect locks/timeouts quickly
                            cmd.CommandTimeout = 5;

                            cmd.Parameters.Add("nc", OracleDbType.Int32).Value = numChckrs;
                            cmd.Parameters.Add("cr", OracleDbType.Varchar2).Value = checkReq;
                            cmd.Parameters.Add("id", OracleDbType.Int32).Value = txnNo;

                            int rowsAffected = cmd.ExecuteNonQuery();

                            if (rowsAffected > 0)
                            {
                                return Json(new { success = true, message = "Update successful!" });
                            }
                            else if (string.IsNullOrEmpty(capability))
                            {
                                return Json(new { success = false, notFound = true, message = "Record not found. Provide details to create." });
                            }
                            else
                            {
                                // 2. INSERT QUERY
                                string insertSql = @"INSERT INTO transactions (TXNNO, TRANSIGN, CASHTOTAL, MNEMONIC, STOREDFORWARD, BATCH, OFFLINEPRINT, DEPOSIT, CASHTRAN, CAPABILITY, TITLE, ALLOWCORRECTION, TOTALLINGIN, SHOWJOURNALRESPONSE, ISFINANCIAL, NUMCHCKRS, CHECKINGREQD, CPC_NUMCHCKRS, CPC_CHECKINGREQD, PRESERVESCRNDATA) 
                                         VALUES (:id, '+', '0000000000', 'CMAP', 0, 0, 0, 0, 0, :cap, :title, 0, 0, 0, :isfin, :nc, :cr, :nc, :cr, 'Y')";

                                using (OracleCommand insCmd = new OracleCommand(insertSql, conn))
                                {
                                    insCmd.BindByName = true;
                                    insCmd.CommandTimeout = 5;
                                    insCmd.Parameters.Add("id", OracleDbType.Int32).Value = txnNo;
                                    insCmd.Parameters.Add("cap", OracleDbType.Varchar2).Value = capability;
                                    insCmd.Parameters.Add("title", OracleDbType.Varchar2).Value = title;
                                    insCmd.Parameters.Add("isfin", OracleDbType.Int32).Value = isFinancial ?? 0;
                                    insCmd.Parameters.Add("nc", OracleDbType.Int32).Value = numChckrs;
                                    insCmd.Parameters.Add("cr", OracleDbType.Varchar2).Value = checkReq;

                                    insCmd.ExecuteNonQuery();
                                }
                                return Json(new { success = true, message = "New record inserted successfully!" });
                            }
                        }
                    }
                    catch (OracleException ex)
                    {
                        string customMessage;
                        switch (ex.Number)
                        {
                            case 54: // ORA-00054: Resource busy (Row Lock)
                                customMessage = "The record is currently locked by another user/session. Please commit any open transactions in your SQL tool.";
                                break;
                            case 1013: // ORA-01013: User requested cancel (often a Timeout)
                            case -2:   // Client-side timeout
                                customMessage = "Database request timed out. The server might be busy or a row lock is being held.";
                                break;
                            case 1: // ORA-00001: Unique constraint violated
                                customMessage = "A record with this Transaction Number already exists.";
                                break;
                            case 12154: // ORA-12154: TNS could not resolve service name
                                customMessage = "Database connection error: Invalid Service Name/Host.";
                                break;
                            default:
                                customMessage = "Oracle Error (ORA-" + ex.Number + "): " + ex.Message;
                                break;
                        }
                        return Json(new { success = false, message = customMessage });
                    }
                    catch (Exception ex)
                    {
                        return Json(new { success = false, message = "System Error: " + ex.Message });
                    }
                }

            }
            else
            {
                return Json(new { success = false, message = "System Error: Empty DB String found, please configue DB string at application level" });
            }
        }

    }
}
