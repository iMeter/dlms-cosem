using System;
using System.IO;
using System.Net.Sockets;
using System.Threading;
using System.Net;
using System.Text;
using System.Data;
using System.Timers;
using System.Collections.Generic;
using Gurux.DLMS;
using System.Runtime.InteropServices;
using Gurux.DLMS.AddIn;
using Gurux.DLMS.Devices;


namespace AsyncServer
{

     public class TcpServer
    {

      //Событие для передачи данных
       public event EventHandler<UserEventArgs> OnDataCompleted;

      public static TcpServer Instance
        {
            get
            {
                return instance;
            }
        }
        private static TcpServer instance = new TcpServer();
        private SocketListener server;
        

        public void Start()
        {
            server = new SocketListener(2000,1024);
            server.Start(4059);
            server.OnDataCompleted += new EventHandler<UserEventArgs>(server_OnDataCompleted);
        }

        void server_OnDataCompleted(object sender, UserEventArgs e)
        {
          if (this.OnDataCompleted != null)
          {
            this.OnDataCompleted(this, e);
          }
        }


      public void Stop()
        {
            server.Stop();
            server.OnDataCompleted -= new EventHandler<UserEventArgs>(server_OnDataCompleted);
        }
    }


    /// <summary>
    /// Based on example from http://msdn2.microsoft.com/en-us/library/system.net.sockets.socketasynceventargs.aspx
    /// Implements the connection logic for the socket server.  
    /// After accepting a connection, all data read from the client is sent back. 
    /// The read and echo back to the client pattern is continued until the client disconnects.
    /// </summary>
    internal sealed class SocketListener
    {

      public event EventHandler<UserEventArgs> OnDataCompleted;
     
      #region Fields

        /// <summary>
        /// The socket used to listen for incoming connection requests.
        /// </summary>
        private Socket listenSocket;

        /// <summary>
        /// Mutex to synchronize server execution.
        /// </summary>
        private static Mutex mutex = new Mutex();

        /// <summary>
        /// Buffer size to use for each socket I/O operation.
        /// </summary>
        private Int32 bufferSize;

         /// <summary>
        /// Pool of reusable SocketAsyncEventArgs objects for write, read and accept socket operations.
        /// </summary>
        private SocketAsyncEventArgsPool readWritePool;

      /// <summary>
        /// The total number of clients connected to the server.
        /// </summary>
        private Int32 numConnectedSockets;

        /// <summary>
        /// the maximum number of connections the sample is designed to handle simultaneously.
        /// </summary>
        private Int32 numConnections;

        /// <summary>
        /// Controls the total number of clients connected to the server.
        /// </summary>
        private Semaphore semaphoreAcceptedClients;

        private const int CLIENT_SOCKET_TIMEOUT = 60000; // 60 sec

        //private ThreadMonitor monitor;

        #endregion

      #region METHODS

        #region LISTENER
        /// <summary>
        /// Create an uninitialized server instance.  
        /// To start the server listening for connection requests
        /// call the Init method followed by Start method.
        /// </summary>
        /// <param name="numConnections">Maximum number of connections to be handled simultaneously.</param>
        /// <param name="bufferSize">Buffer size to use for each socket I/O operation.</param>
        public SocketListener(Int32 numConnections, Int32 bufferSize)
        {
          this.numConnectedSockets = 0;
          this.numConnections = numConnections;
          this.bufferSize = bufferSize;

          this.readWritePool = new SocketAsyncEventArgsPool(numConnections);
          this.semaphoreAcceptedClients = new Semaphore(numConnections, numConnections);

          // Preallocate pool of SocketAsyncEventArgs objects.
          for (Int32 i = 0; i < this.numConnections; i++)
          {
            SocketAsyncEventArgs readWriteEventArg = new SocketAsyncEventArgs();
            readWriteEventArg.Completed += new EventHandler<SocketAsyncEventArgs>(readWriteEventArg_Completed);
            //readWriteEventArg.SetBuffer(new Byte[this.bufferSize], 0, this.bufferSize);

            // Add SocketAsyncEventArg to the pool.
            this.readWritePool.Push(readWriteEventArg);
          }
        }
      
        /// <summary>
        /// Starts the server listening for incoming connection requests.
        /// </summary>
        /// <param name="port">Port where the server will listen for connection requests.</param>
        internal void Start(Int32 port)
        {
            Stop();

            // Get host related information.
            IPAddress[] addressList = Dns.GetHostEntry(Environment.MachineName).AddressList;

            // Get endpoint for the listener.
            IPEndPoint localEndPoint = new IPEndPoint(addressList[addressList.Length - 1], port);

            // Create the socket which listens for incoming connections.
            this.listenSocket = new Socket(localEndPoint.AddressFamily, SocketType.Stream, ProtocolType.Tcp); 
            this.listenSocket.ReceiveBufferSize = this.bufferSize;
            this.listenSocket.SendBufferSize = this.bufferSize;

            if (localEndPoint.AddressFamily == AddressFamily.InterNetworkV6)
            {
                // Set dual-mode (IPv4 & IPv6) for the socket listener.
                // 27 is equivalent to IPV6_V6ONLY socket option in the winsock snippet below,
                // based on http://blogs.msdn.com/wndp/archive/2006/10/24/creating-ip-agnostic-applications-part-2-dual-mode-sockets.aspx
                this.listenSocket.SetSocketOption(SocketOptionLevel.IPv6, (SocketOptionName)27, false);
                this.listenSocket.Bind(new IPEndPoint(IPAddress.IPv6Any, localEndPoint.Port));
            }
            else
            {
                // Associate the socket with the local endpoint.
                this.listenSocket.Bind(localEndPoint);
            }
              // Start the server.
              this.listenSocket.Listen(this.numConnections);

              // Post accepts on the listening socket.
              this.StartAccept(null);


            //monitor = new ThreadMonitor(readWritePool.poolUsed, CloseSession, CLIENT_SOCKET_TIMEOUT);
            //monitor.Enable();

            // Blocks the current thread to receive incoming messages.
            mutex.WaitOne();
        }

        /// <summary>
        /// Stop the server.
        /// </summary>
        internal void Stop()
        {
            if (this.listenSocket != null)
            {
                this.listenSocket.Close();
                this.listenSocket = null;
                //monitor.Disable();
                mutex.ReleaseMutex();
            }
        }
        #endregion

        #region ACCEPT
        /// <summary>
        /// Begins an operation to accept a connection request from the client.
        /// </summary>
        /// <param name="acceptEventArg">The context object to use when issuing 
        /// the accept operation on the server's listening socket.</param>
        private void StartAccept(SocketAsyncEventArgs acceptEventArg)
        {
            if (acceptEventArg == null)
            {
                acceptEventArg = new SocketAsyncEventArgs();
                acceptEventArg.Completed += new EventHandler<SocketAsyncEventArgs>(OnAcceptCompleted);
            }
            else
            {
                // Socket must be cleared since the context object is being reused.
                acceptEventArg.AcceptSocket = null;
            }

            this.semaphoreAcceptedClients.WaitOne();
            if (!this.listenSocket.AcceptAsync(acceptEventArg))
            {
                this.ProcessAccept(acceptEventArg);
            }
        }

        /// <summary>
        /// Callback method associated with Socket.AcceptAsync 
        /// operations and is invoked when an accept operation is complete.
        /// </summary>
        /// <param name="sender">Object who raised the event.</param>
        /// <param name="e">SocketAsyncEventArg associated with the completed accept operation.</param>
        private void OnAcceptCompleted(object sender, SocketAsyncEventArgs e)
        {
            this.ProcessAccept(e);
        }

        /// <summary>
        /// Process the accept for the socket listener.
        /// </summary>
        /// <param name="e">SocketAsyncEventArg associated with the completed accept operation.</param>
        private void ProcessAccept(SocketAsyncEventArgs e)
        {
            Socket s = e.AcceptSocket;
            if (s.Connected)
            {
                try
                {
                    SocketAsyncEventArgs clientArgs = this.readWritePool.Pop();
                    if (clientArgs != null)
                    {
                      var session = new Session(e.AcceptSocket);
                      clientArgs.UserToken = session;
                        Interlocked.Increment(ref this.numConnectedSockets);
                        Console.WriteLine("Client connection accepted. There are {0} clients connected to the server",
                            this.numConnectedSockets);
                        Thread.Sleep(1000);
                        //session.OnSessionCompilete += new Session.SessionCompilete(session_OnSessionCompilete);
                        session.sendDataFromClass += new EventHandler<UserEventArgs>(session_sendDataFromClass);
                        
                      session.Send(clientArgs); //only DLMS !!!
                        //session.Receive(clientArgs);
                    }
                    else
                    {
                        Console.WriteLine("There are no more available sockets to allocate.");
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.ToString());
                }

                // Accept the next connection request.
                this.StartAccept(e);
            }
        }

        void session_sendDataFromClass(object sender, UserEventArgs e)
        {
          if (OnDataCompleted != null)
            OnDataCompleted(this, e);
        }

        void session_OnSessionCompilete(GFTables sender, EventArgs e)
        {
          DataSet ds = new DataSet();
          foreach( Gurux.DLMS.AddIn.GXTable tab in sender )
          {
            DataTable tbl = null;
            if (tab.LogicalName == "7.0.99.99.2.255")
            {
              tbl = new DataTable("Hourly");
            }
            else if (tab.LogicalName == "7.0.99.99.3.255")
            {
              tbl = new DataTable("Daily");
            }
            else if (tab.LogicalName == "7.0.99.99.4.255")
            {
              tbl = new DataTable("Monthly");
            }
            else if (tab.LogicalName == "7.0.99.98.1.255")
            {
              tbl = new DataTable("Alarm");
            }
            else if (tab.LogicalName == "7.0.99.98.0.255")
            {
              tbl = new DataTable("Event");
            }
            else if (tab.LogicalName == "7.0.99.98.2.255")
            {
              tbl = new DataTable("Audit");
            }
            if (tbl != null)
            {
              for (int i = 0; i < tab.Columns.Count; ++i)
              {
                //tbl.Columns.Add(tab.Columns[i].Name, tab.Columns[i].ValueType);
                tbl.Columns.Add(tab.Columns[i].Name);
              }
              List<object[]> rows = tab.GetRows(0, tab.RowCount, false);
              foreach (object[] row in rows) 
              {
                tbl.Rows.Add(row);
              }
           

              ds.Tables.Add(tbl);
            }
          }
          if (OnDataCompleted != null)
            OnDataCompleted(this, new UserEventArgs(ds) );
          //var form = new Gurux.DLMS.AsyncServer.xFrmView(ds);
          //form.ShowDialog();
        }

  
        #endregion

        #region Event Invocators

        private void readWriteEventArg_Completed(object sender, SocketAsyncEventArgs e)
        {
          bool err = false;
          var session = e.UserToken as Session;
          // Determine which type of operation just completed and call the associated handler.
          if (e.SocketError == SocketError.ConnectionReset || e.SocketError == SocketError.Disconnecting )
          {
            err = true;
            Console.WriteLine("Remote client socket reset");
          }
          else
          {
            switch (e.LastOperation)
            {
              case SocketAsyncOperation.Receive:
                err = session.ProcessReceive(e);
                break;
              case SocketAsyncOperation.Send:
                err = session.ProcessSent(e);
                break;
              case SocketAsyncOperation.Disconnect:
                err = true;
                break;
              default:
                throw new ArgumentException("The last operation completed on the socket was not a receive or send");
            }
          }
          if (err)
          {
            //Thread.Sleep(1000);
            CloseSession(e);
          }
        }
        #endregion
        
        #region CLOSE
        void CloseSession(SocketAsyncEventArgs e)
        {
          //monitor.Disable();
          var session = e.UserToken as Session;
          session.sendDataFromClass -= new EventHandler<UserEventArgs>(session_sendDataFromClass);

          if (session != null)
          {
              session.Dispose();
              session = null;
          }
          //monitor.Enable();
          // decrement the counter keeping track of the total number of clients connected to the server
          Interlocked.Decrement(ref numConnectedSockets);
          semaphoreAcceptedClients.Release();
          Console.WriteLine("A client has been disconnected from the server. There are {0} clients connected to the server", numConnectedSockets);

          // Free the SocketAsyncEventArg so they can be reused by another client
          this.readWritePool.Push(e);
        }
        #endregion


     #endregion
    }

}
