using BancsEventsLogger.Models;
using System;
using System.Collections.Concurrent;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Web.Mvc;

namespace BancsEventsLogger.Controllers
{
    public class SocketClientController : Controller
    {
        #region Connection Store

        private static readonly ConcurrentDictionary<string, SocketConnection>
            _connections =
                new ConcurrentDictionary<string, SocketConnection>();


        private SocketConnection CurrentConnection
        {
            get
            {
                return _connections.GetOrAdd(
                  ConnectionKey,
                  id => new SocketConnection());
            }
        }

        #endregion

        #region View

        public ActionResult SocketClient()
        {
            return View();
        }

        #endregion

        #region Helper Methods

        private bool IsConnected()
        {
            return CurrentConnection.ClientSocket != null && CurrentConnection.IsConnected;
        }

        private SocketResponseModel BuildResponse(
            bool success,
            string message,
            string response = "")
        {
            return new SocketResponseModel
            {
                Success = success,

                SuccessMessage = message,

                ResponseData = response,

                ConnectionStatus =
                    IsConnected()
                        ? "Connected"
                        : "Disconnected",

                TimeStamp = DateTime.Now
                    .ToString("dd-MMM-yyyy HH:mm:ss")
            };
        }

        private SocketStateModel BuildState()
        {
            return new SocketStateModel
            {
                IsConnected = IsConnected(),

                ServerIP = string.Empty,

                Port = 0,

                TxCount = CurrentConnection.TxCount,

                RxCount = CurrentConnection.RxCount,

                LastRequest = CurrentConnection.LastRequest,

                LastResponse = CurrentConnection.LastResponse,

                LastActivity = CurrentConnection.LastActivity
            };
        }

        #endregion

        #region Send

        [HttpPost]
        public JsonResult Send(SocketRequestModel model)
        {
            try
            {
                if (!IsConnected())
                    return Json(BuildResponse(false, "Socket is not connected."));

                if (model == null)
                    return Json(BuildResponse(false, "Invalid request."));

                if (string.IsNullOrWhiteSpace(model.HostMessage))
                    return Json(BuildResponse(false, "Request message cannot be empty."));

                Socket socket = CurrentConnection.ClientSocket;

                NetworkStream networkStream = new NetworkStream(socket, false);

                StreamWriter writer = new StreamWriter(networkStream, Encoding.ASCII);

                writer.AutoFlush = true;

                writer.Write(model.HostMessage);
                // System.Text.Encoding.ASCII.GetBytes

                writer.Flush();

                CurrentConnection.LastRequest = model.HostMessage;
                CurrentConnection.TxCount++;
                CurrentConnection.LastActivity = DateTime.Now;

                return Json(new SocketResponseModel
                {
                    Success = true,
                    SuccessMessage = "Request sent successfully.",
                    ConnectionStatus = "Connected",
                    TimeStamp = DateTime.Now.ToString("dd-MMM-yyyy HH:mm:ss")
                });
            }
            catch (SocketException ex)
            {
                CloseConnection();
                return Json(BuildResponse(false, ex.Message));
            }
            catch (Exception ex)
            {
                CloseConnection();
                return Json(BuildResponse(false, ex.Message));
            }
        }

        #endregion

        #region Connect

        [HttpPost]
        public JsonResult Connect(SocketRequestModel model)
        {
            try
            {
                if (model == null)
                    return Json(BuildResponse(false, "Invalid request."));

                if (string.IsNullOrWhiteSpace(model.HostAddress))
                    return Json(BuildResponse(false, "Server IP Address is required."));

                if (model.HostPort <= 0)
                    return Json(BuildResponse(false, "Invalid Port Number."));

                if (IsConnected())
                    return Json(BuildResponse(true, "Already Connected."));

                IPAddress ipAddress;

                if (!IPAddress.TryParse(model.HostAddress, out ipAddress))
                    return Json(BuildResponse(false, "Invalid IP Address."));

                Socket socket = new Socket(
                  AddressFamily.InterNetwork,
                  SocketType.Stream,
                  ProtocolType.Tcp);

                socket.NoDelay = true;
                socket.SendTimeout = 30000;
                socket.ReceiveTimeout = 30000;

                socket.Connect(new IPEndPoint(ipAddress, model.HostPort));

                if (!socket.Connected)
                {
                    socket.Dispose();
                    return Json(BuildResponse(false, "Unable to connect to server."));
                }

                CurrentConnection.ClientSocket = socket;
                CurrentConnection.IsConnected = true;
                CurrentConnection.TxCount = 0;
                CurrentConnection.RxCount = 0;
                CurrentConnection.LastRequest = string.Empty;
                CurrentConnection.LastResponse = string.Empty;
                CurrentConnection.LastActivity = DateTime.Now;

                // Start asynchronous receive
                WaitForData();

                return Json(new SocketResponseModel
                {
                    Success = true,
                    SuccessMessage = "Connected Successfully.",
                    ConnectionStatus = "Connected",
                    TimeStamp = DateTime.Now.ToString("dd-MMM-yyyy HH:mm:ss")
                });
            }
            catch (SocketException ex)
            {
                CloseConnection();

                return Json(BuildResponse(false, ex.Message));
            }
            catch (Exception ex)
            {
                CloseConnection();

                return Json(BuildResponse(false, ex.Message));
            }
        }

        #endregion

        #region WaitForData

        private void WaitForData()
        {
            try
            {
                if (!IsConnected())
                    return;

                if (CurrentConnection.ReceiveCallback == null)
                {
                    CurrentConnection.ReceiveCallback =
                      new AsyncCallback(OnDataReceived);
                }

                SocketPacket packet = new SocketPacket();

                packet.ThisSocket = CurrentConnection.ClientSocket;

                packet.Connection = CurrentConnection;

                packet.ThisSocket.BeginReceive(
                  packet.DataBuffer,
                  0,
                  packet.DataBuffer.Length,
                  SocketFlags.None,
                  CurrentConnection.ReceiveCallback,
                  packet);
            }
            catch (SocketException)
            {
                CloseConnection();
            }
            catch (ObjectDisposedException)
            {
                CloseConnection();
            }
            catch (Exception)
            {
                CloseConnection();
            }
        }

        #endregion

        #region OnDataReceived

        private void OnDataReceived(IAsyncResult ar)
        {
            SocketPacket packet = null;

            try
            {
                packet = (SocketPacket)ar.AsyncState;

                if (packet == null || packet.ThisSocket == null)
                    return;

                int bytesRead = packet.ThisSocket.EndReceive(ar);

                if (bytesRead <= 0)
                {
                    CloseConnection();
                    return;
                }

                string response =
                    Encoding.ASCII.GetString(
                        packet.DataBuffer,
                        0,
                        bytesRead);

                packet.Connection.LastResponse = response;

                packet.Connection.RxCount++;

                packet.Connection.LastActivity = DateTime.Now;

                // Continue listening for next response
                WaitForData();
            }
            catch (ObjectDisposedException)
            {
                CloseConnection();
            }
            catch (SocketException)
            {
                CloseConnection();
            }
            catch (Exception ex)
            {
                if (packet != null && packet.Connection != null)
                {
                    packet.Connection.LastResponse =
                        "ERROR : " + ex.Message;
                }

                CloseConnection();
            }
        }

        #endregion

        #region GetLatestResponse

        [HttpGet]
        public JsonResult GetLatestResponse()
        {
            try
            {
                string response = CurrentConnection.LastResponse;

                if (!string.IsNullOrEmpty(response))
                {
                    CurrentConnection.LastResponse = string.Empty;
                }

                return Json(
                  new SocketResponseModel
                  {
                      Success = true,
                      ResponseData = response,
                      ConnectionStatus =
                      IsConnected()
                        ? "Connected"
                        : "Disconnected",

                      TimeStamp = DateTime.Now
                      .ToString("dd-MMM-yyyy HH:mm:ss")
                  },
                  JsonRequestBehavior.AllowGet);
            }
            catch (Exception ex)
            {
                return Json(
                  BuildResponse(false, ex.Message),
                  JsonRequestBehavior.AllowGet);
            }
        }

        #endregion

        #region Disconnect

        [HttpPost]
        public JsonResult Disconnect()
        {
            try
            {
                CloseConnection();

                return Json(new SocketResponseModel
                {
                    Success = true,
                    SuccessMessage = "Disconnected successfully.",
                    ConnectionStatus = "Disconnected",
                    TimeStamp = DateTime.Now.ToString("dd-MMM-yyyy HH:mm:ss")
                });
            }
            catch (Exception ex)
            {
                return Json(BuildResponse(false, ex.Message));
            }
        }

        #endregion

        #region CloseConnection

        private void CloseConnection()
        {
            try
            {
                if (CurrentConnection.ClientSocket != null)
                {
                    try
                    {
                        if (CurrentConnection.ClientSocket.Connected)
                        {
                            CurrentConnection.ClientSocket.Shutdown(
                                SocketShutdown.Both);
                        }
                    }
                    catch
                    {
                        // Ignore shutdown errors
                    }

                    try
                    {
                        CurrentConnection.ClientSocket.Close();
                    }
                    catch
                    {
                    }

                    try
                    {
                        CurrentConnection.ClientSocket.Dispose();
                    }
                    catch
                    {
                    }
                }

                CurrentConnection.ClientSocket = null;
                CurrentConnection.IsConnected = false;
                CurrentConnection.TxCount = 0;
                CurrentConnection.RxCount = 0;
                CurrentConnection.LastRequest = string.Empty;
                CurrentConnection.LastResponse = string.Empty;
                CurrentConnection.LastActivity = null;
                CurrentConnection.ReceiveCallback = null;
            }
            catch
            {
                // Ignore cleanup exceptions
            }
        }

        #endregion

        /* Old Not Working Code Just Kept for Reference*/
        /*private string ConnectionKey
        {
            get
            {
                if (Session["ConnectionKey"] == null)
                {
                    Session["ConnectionKey"] = Guid.NewGuid().ToString();
                }

                return Session["ConnectionKey"].ToString();
            }
        }*/
        /* Old Not Working Code Just Kept for Reference*/
        private string ConnectionKey
        {
            get
            {
                if (System.Web.HttpContext.Current?.Session == null)
                {
                    return string.Empty;
                }
                if (Session["ConnectionKey"] == null)
                {
                    Session["ConnectionKey"] = Guid.NewGuid().ToString();
                }
                return Convert.ToString(Session["ConnectionKey"]);
            }
        }
    }
}
