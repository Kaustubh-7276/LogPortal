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
        private static Socket _clientSocket;
        public byte[] dataBuffer = new byte[1024];
        private static StreamReader _reader;
        private static StreamWriter _writer;
        private static NetworkStream _networkStream;

        public class SocketPacket
        {
            public Socket thisSocket;


        }
        public ActionResult SocketClient()
        {
            return View();
        }

        private bool IsConnected()
        {
            return _clientSocket != null &&
               _clientSocket.Connected;
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
                ConnectionStatus = IsConnected()
                        ? "Connected"
                        : "Disconnected",
                TimeStamp = DateTime.Now.ToString("dd-MMM-yyyy HH:mm:ss")
            };
        }
        private void CloseConnection()
        {
            try
            {
                if (_writer != null)
                {
                    _writer.Close();
                    _writer.Dispose();
                    _writer = null;
                }

                if (_reader != null)
                {
                    _reader.Close();
                    _reader.Dispose();
                    _reader = null;
                }

                if (_networkStream != null)
                {
                    _networkStream.Close();
                    _networkStream.Dispose();
                    _networkStream = null;
                }

                if (_clientSocket != null)
                {
                    if (_clientSocket.Connected)
                    {
                        _clientSocket.Shutdown(SocketShutdown.Both);
                    }

                    _clientSocket.Close();
                    _clientSocket.Dispose();
                    _clientSocket = null;
                }
            }
            catch
            {
                // Ignore cleanup exceptions
            }
        }

        //private string ReceiveResponse()
        //{
        //    try
        //    {
        //        StringBuilder response = new StringBuilder();

        //        char[] buffer = new char[4096];

        //        int bytesRead = _reader.Read(buffer, 0, buffer.Length);

        //        if (bytesRead > 0)
        //        {
        //            response.Append(buffer, 0, bytesRead);
        //        }

        //        return response.ToString();
        //    }
        //    catch
        //    {
        //        throw;
        //    }
        //}

        private string ReceiveResponse()
        {
            try
            {
                byte[] buffer = new byte[4096];
                int count = _clientSocket.Receive(buffer);
                if (count > 0)
                {
                    return string.Empty;
                }
                return Encoding.UTF8.GetString(buffer, 0, count);
            }
            catch
            {
                throw;
            }
        }

        [HttpPost]
        public JsonResult Connect(SocketRequestModel model)
        {
            try
            {
                if (model == null)
                {
                    return Json(BuildResponse(false, "Invalid request."));
                }

                if (string.IsNullOrWhiteSpace(model.HostAddress))
                {
                    return Json(BuildResponse(false, "Server IP Address is required."));
                }

                if (model.HostPort <= 0)
                {
                    return Json(BuildResponse(false, "Invalid Port Number."));
                }

                if (IsConnected())
                {
                    return Json(BuildResponse(true, "Already Connected."));
                }

                _clientSocket = new Socket(
                          AddressFamily.InterNetwork,
                          SocketType.Stream,
                          ProtocolType.Tcp);

                _clientSocket.ReceiveTimeout = 30000;
                _clientSocket.SendTimeout = 30000;

                IPAddress ipAddress;

                if (!IPAddress.TryParse(model.HostAddress, out ipAddress))
                {
                    return Json(BuildResponse(false, "Invalid IP Address."));
                }

                IPEndPoint endPoint =
                  new IPEndPoint(ipAddress, model.HostPort);

                _clientSocket.Connect(endPoint);

                if (!_clientSocket.Connected)
                {
                    return Json(BuildResponse(false,
                          "Unable to connect to server."));
                }

                _networkStream = new NetworkStream(
                            _clientSocket,
                            true);

                _reader = new StreamReader(
                        _networkStream,
                        Encoding.UTF8);

                _writer = new StreamWriter(
                        _networkStream,
                        Encoding.UTF8);

                _writer.AutoFlush = true;

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

                return Json(BuildResponse(false,
                      "Socket Error : " + ex.Message));
            }
            catch (Exception ex)
            {
                CloseConnection();

                return Json(BuildResponse(false,
                      ex.Message));
            }
        }

        [HttpPost]
        public JsonResult Send(SocketRequestModel model)
        {
            try
            {
                if (!IsConnected())
                {
                    return Json(BuildResponse(false,
                          "Socket is not connected."));
                }

                if (model == null)
                {
                    return Json(BuildResponse(false,
                          "Invalid Request."));
                }

                if (string.IsNullOrWhiteSpace(model.HostMessage))
                {
                    return Json(BuildResponse(false,
                          "Request Message cannot be empty."));
                }

                // Send request to server
                _writer.WriteLine(model.HostMessage);
                _writer.Flush();

                // Receive response
                string response = ReceiveResponse();

                return Json(new SocketResponseModel
                {
                    Success = true,
                    SuccessMessage = "Request processed successfully.",
                    ResponseData = response,
                    ConnectionStatus = "Connected",
                    TimeStamp = DateTime.Now.ToString("dd-MMM-yyyy HH:mm:ss")
                });
            }
            catch (SocketException ex)
            {
                CloseConnection();

                return Json(BuildResponse(false,
                      "Socket Error : " + ex.Message));
            }
            catch (IOException ex)
            {
                CloseConnection();

                return Json(BuildResponse(false,
                      "Communication Error : " + ex.Message));
            }
            catch (Exception ex)
            {
                CloseConnection();

                return Json(BuildResponse(false,
                      ex.Message));
            }
        }

        [HttpPost]
        public JsonResult Disconnect()
        {
            try
            {
                CloseConnection();

                return Json(new SocketResponseModel
                {
                    Success = true,
                    SuccessMessage = "Disconnected Successfully.",
                    ConnectionStatus = "Disconnected",
                    TimeStamp = DateTime.Now.ToString("dd-MMM-yyyy HH:mm:ss")
                });
            }
            catch (Exception ex)
            {
                return Json(BuildResponse(false, ex.Message));
            }
        }

        [HttpGet]
        public JsonResult GetConnectionStatus()
        {
            return Json(new SocketResponseModel
            {
                Success = true,
                ConnectionStatus = IsConnected()
                        ? "Connected"
                        : "Disconnected",
                TimeStamp = DateTime.Now.ToString("dd-MMM-yyyy HH:mm:ss")
            },
            JsonRequestBehavior.AllowGet);
        }

        [HttpGet]
        public JsonResult Ping()
        {
            try
            {
                if (!IsConnected())
                {
                    return Json(BuildResponse(false,
                      "Socket not connected."),
                      JsonRequestBehavior.AllowGet);
                }

                return Json(BuildResponse(true,
                  "Socket Connected."),
                  JsonRequestBehavior.AllowGet);
            }
            catch (Exception ex)
            {
                return Json(BuildResponse(false,
                  ex.Message),
                  JsonRequestBehavior.AllowGet);
            }
        }

        private bool ValidateConnection()
        {
            if (_clientSocket == null)
                return false;

            if (!_clientSocket.Connected)
                return false;

            return true;
        }

        private void WriteData(string message)
        {
            if (!ValidateConnection())
                throw new Exception("Socket is disconnected.");

            _writer.WriteLine(message);
            _writer.Flush();
        }

        private string ReadData()
        {
            if (!ValidateConnection())
                throw new Exception("Socket is disconnected.");

            char[] buffer = new char[4096];

            int count = _reader.Read(buffer, 0, buffer.Length);

            if (count <= 0)
                return String.Empty;

            return new string(buffer, 0, count);
        }

    }
}
