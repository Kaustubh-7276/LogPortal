using BancsEventsLogger.Models;
using System;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Web.Mvc;

namespace BancsEventsLogger.Controllers
{
    public class SocketClientController : Controller
    {
        #region Socket Members

        private static Socket _clientSocket;

        private static NetworkStream _networkStream;

        private static StreamWriter _writer;

        private static readonly byte[] _buffer = new byte[4096];

        private static AsyncCallback _receiveCallback;

        private static readonly object _lock = new object();

        #endregion

        #region Runtime State

        private static bool _isConnected;

        private static string _latestResponse = String.Empty;

        private static string _lastRequest = String.Empty;

        private static int _txCount;

        private static int _rxCount;

        private static DateTime? _lastActivity;

        #endregion
        public ActionResult SocketClient()
        {
            return View();
        }

        private bool IsConnected()
        {
            return _clientSocket != null
              && _clientSocket.Connected
              && _isConnected;
        }

        private SocketResponseModel BuildResponse(bool success, string message, string response = "")
        {
            return new SocketResponseModel
            {
                Success = success,
                SuccessMessage = message,
                ResponseData = response,
                ConnectionStatus = IsConnected()
                ? "Connected"
                : "Disconnected",
                TimeStamp = DateTime.Now
                .ToString("dd-MMM-yyyy HH:mm:ss")
            };
        }


        #region Connect

        [HttpPost]
        public JsonResult Connect(SocketRequestModel model)
        {
            lock (_lock)
            {
                try
                {
                    if (model == null)
                        return Json(BuildResponse(false, "Invalid request."));

                    if (String.IsNullOrWhiteSpace(model.HostAddress))
                        return Json(BuildResponse(false, "Server IP Address is required."));

                    if (model.HostPort <= 0)
                        return Json(BuildResponse(false, "Invalid Port Number."));

                    if (IsConnected())
                        return Json(BuildResponse(true, "Already Connected."));

                    _clientSocket = new Socket(
                      AddressFamily.InterNetwork,
                      SocketType.Stream,
                      ProtocolType.Tcp);

                    _clientSocket.ReceiveTimeout = 30000;
                    _clientSocket.SendTimeout = 30000;

                    IPAddress ipAddress;

                    if (!IPAddress.TryParse(model.HostAddress, out ipAddress))
                        return Json(BuildResponse(false, "Invalid IP Address."));

                    IPEndPoint endPoint =
                      new IPEndPoint(ipAddress, model.HostPort);

                    _clientSocket.Connect(endPoint);

                    if (!_clientSocket.Connected)
                        return Json(BuildResponse(false,
                          "Unable to connect to server."));

                    _networkStream =
                      new NetworkStream(_clientSocket, false);

                    _writer =
                      new StreamWriter(_networkStream, Encoding.UTF8);

                    _writer.AutoFlush = true;

                    _latestResponse = String.Empty;

                    _lastRequest = String.Empty;

                    _txCount = 0;

                    _rxCount = 0;

                    _lastActivity = DateTime.Now;

                    _isConnected = true;

                    WaitForData();

                    return Json(BuildResponse(
                      true,
                      "Connected Successfully."));
                }
                catch (SocketException ex)
                {
                    CloseConnection();

                    return Json(BuildResponse(
                      false,
                      ex.Message));
                }
                catch (Exception ex)
                {
                    CloseConnection();

                    return Json(BuildResponse(
                      false,
                      ex.Message));
                }
            }
        }

        #endregion

        #region Wait For Data

        private void WaitForData()
        {
            try
            {
                if (!IsConnected())
                    return;

                if (_receiveCallback == null)
                    _receiveCallback =
                      new AsyncCallback(OnDataReceived);

                _clientSocket.BeginReceive(
                  _buffer,
                  0,
                  _buffer.Length,
                  SocketFlags.None,
                  _receiveCallback,
                  null);
            }
            catch
            {
                CloseConnection();
            }
        }

        #endregion

        #region Receive Callback

        private void OnDataReceived(IAsyncResult ar)
        {
            try
            {
                if (!IsConnected())
                    return;

                int bytesRead = _clientSocket.EndReceive(ar);

                if (bytesRead <= 0)
                {
                    CloseConnection();
                    return;
                }

                string response =
                  Encoding.UTF8.GetString(
                    _buffer,
                    0,
                    bytesRead);

                lock (_lock)
                {
                    _latestResponse = response;

                    _rxCount++;

                    _lastActivity = DateTime.Now;
                }

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
            catch (Exception)
            {
                CloseConnection();
            }
        }

        #endregion

        #region Send

        [HttpPost]
        public JsonResult Send(SocketRequestModel model)
        {
            lock (_lock)
            {
                try
                {
                    if (!IsConnected())
                        return Json(BuildResponse(false,
                          "Socket is not connected."));

                    if (model == null)
                        return Json(BuildResponse(false,
                          "Invalid Request."));

                    if (String.IsNullOrWhiteSpace(model.HostMessage))
                        return Json(BuildResponse(false,
                          "Request message cannot be empty."));

                    _writer.WriteLine(model.HostMessage);

                    _writer.Flush();

                    _lastRequest = model.HostMessage;

                    _txCount++;

                    _lastActivity = DateTime.Now;

                    return Json(new SocketResponseModel
                    {
                        Success = true,
                        SuccessMessage = "Request sent successfully.",
                        ConnectionStatus = "Connected",
                        TimeStamp = DateTime.Now
                        .ToString("dd-MMM-yyyy HH:mm:ss")
                    });
                }
                catch (Exception ex)
                {
                    CloseConnection();

                    return Json(BuildResponse(false,
                      ex.Message));
                }
            }
        }

        #endregion

        #region Close Connection

        private void CloseConnection()
        {
            try
            {
                _isConnected = false;

                if (_writer != null)
                {
                    _writer.Dispose();
                    _writer = null;
                }

                if (_networkStream != null)
                {
                    _networkStream.Dispose();
                    _networkStream = null;
                }

                if (_clientSocket != null)
                {
                    try
                    {
                        if (_clientSocket.Connected)
                        {
                            _clientSocket.Shutdown(SocketShutdown.Both);
                        }
                    }
                    catch
                    {
                    }

                    _clientSocket.Close();
                    _clientSocket.Dispose();
                    _clientSocket = null;
                }
            }
            finally
            {
                _latestResponse = String.Empty;

                _lastRequest = String.Empty;

                _txCount = 0;

                _rxCount = 0;

                _lastActivity = null;
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

                return Json(BuildResponse(
                  true,
                  "Disconnected Successfully."));
            }
            catch (Exception ex)
            {
                return Json(BuildResponse(false,
                  ex.Message));
            }
        }

        #endregion

        #region Latest Response

        [HttpGet]
        public JsonResult GetLatestResponse()
        {
            lock (_lock)
            {
                return Json(new SocketResponseModel
                {
                    Success = true,

                    ResponseData = _latestResponse,

                    ConnectionStatus =
                    IsConnected()
                      ? "Connected"
                      : "Disconnected",

                    TimeStamp = _lastActivity.HasValue
                    ? _lastActivity.Value.ToString("dd-MMM-yyyy HH:mm:ss")
                    : ""
                },
                JsonRequestBehavior.AllowGet);
            }
        }

        #endregion

        #region Connection Status

        [HttpGet]
        public JsonResult GetConnectionStatus()
        {
            return Json(new SocketResponseModel
            {
                Success = true,

                ConnectionStatus =
                IsConnected()
                  ? "Connected"
                  : "Disconnected",

                TimeStamp = DateTime.Now
                .ToString("dd-MMM-yyyy HH:mm:ss")
            },
            JsonRequestBehavior.AllowGet);
        }

        #endregion

        #region Ping

        [HttpGet]
        public JsonResult Ping()
        {
            return Json(BuildResponse(
              IsConnected(),
              IsConnected()
                ? "Socket Connected."
                : "Socket Disconnected."),
              JsonRequestBehavior.AllowGet);
        }

        #endregion

    }
}
