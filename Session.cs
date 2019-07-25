using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Gurux.DLMS;
using System.IO.Ports;
using Gurux.DLMS.ManufacturerSettings;
using Gurux.DLMS.Objects;
using System.IO;
using System.Net.Sockets;
using System.Net;
using System.Xml.Serialization;
using System.Threading;
using System.Windows.Forms;
using Gurux.DLMS.AddIn;
using Gurux.DLMS.Devices;
using System.Data;

namespace AsyncServer
{

  enum ClientState
  {
    Unknown = 0,
    Identification = 1,
  };

  enum CosemState
  {
    Idle = 0,
    Indication,
    Authentication,
    Relese,
    Closing,
    Closed
  };

  enum ProcessState
  {
    Idle = 0,
    GetAvailableObjects,
    GetAttribute,
    GetProfileGeneric,

  };

 
  /*class OtherClass
  {
    //private поле таблицы
    private DataSet tableFromClass;

    //Событие для передачи данных
    public event EventHandler<UserEventArgs> sendDataFromClass;

    //Конструктор класса
    public OtherClass(GFTables tabs)
    {
      //Метод формирования таблицы
      tableFromClass = GetDataset(tabs);
    }

    //public метод отправки данных
    public void SendTableToForm()
    {
      //Генерируем событие с именованным аргументом
      //в класс аргумента передаем созданную таблицу
      if (sendDataFromClass != null)
        sendDataFromClass(this, new UserEventArgs(tableFromClass));
    }

    //private метод формирования таблицы
    private DataSet GetDataset(GFTables tabs)
    {
      //Создаем таблицу
      DataSet ds = new DataSet();
      foreach (Gurux.DLMS.AddIn.GXTable tab in tabs)
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
      } return ds;
    }
  }
*/
  class Session : IDisposable
  {

    delegate bool MethodReceive(SocketAsyncEventArgs e);
    delegate bool MethodSend(SocketAsyncEventArgs e);

    //public delegate void SessionCompilete(GFTables sender, EventArgs e);
    //public event SessionCompilete OnSessionCompilete; 

    //Событие для передачи данных
    public event EventHandler<UserEventArgs> sendDataFromClass;
 
    public Socket Socket;
    public int LastActive;


    public DLMSDevice Device
    {
      get { return m_Device; }
      private set { m_Device = value; }
    }
    
    // all data from meter
    private GXDLMSClient m_Client = null; // it's device context
    private GXManufacturer m_Manufacturer = null;
    private DLMSDevice m_Device = null;
    private DateTime LastDateTime;

    public GFTables Tables = new GFTables();

    struct TCount
    {
        public int hour, day, alarm, audit;
        public void Clear() { hour = day = alarm = audit = 0; }
    };
    TCount Count;


    // these variables to handler 
    private bool trace = true;
    private StreamWriter logFile;
    private ClientState ClientState = ClientState.Unknown; 
    private CosemState CosemState = CosemState.Idle;
    private ProcessState ProcessState = ProcessState.Idle;
    private byte[] Header = new byte[64];
    private const int HeaderLengthRGK = 6;
    private const int HeaderLengthDLMS = 8;
    private byte[] Data;
    private UInt16 DataLength;
    private int BytesReceived = 0;
    private byte[] allData = null;
    private RequestTypes moredata = RequestTypes.None;
    private bool IsHeader = true;
    private GXDLMSObject CurrentObject;
    private int[] CurrentObjectAttrs;
    private int ObjectIndex, AttrIndex;

    //private Array InfoColumns;
    //private List<GXKeyValuePair<GXDLMSObject, GXDLMSCaptureObject>> TableColumns;

    /// <summary>
    /// Constructor.
    /// </summary>                                   
    public Session(Socket sock)
    {
      m_Client = new GXDLMSClient(true, 1, 1, Authentication.None, "", InterfaceType.Net);
      Socket = sock;

      //Delete trace file if exists.
      if (File.Exists("LogFiles\\trace.txt"))
      {
        File.Delete("LogFiles\\trace.txt");
      }
      //this is used by the timeout monitor
      LastActive = Environment.TickCount;
      Data = CosemInit(null);                    //only DLMS !!!

      logFile = new StreamWriter(File.Open("LogFiles\\LogFile.txt", FileMode.Create));
      logFile.AutoFlush = true;
      Count.Clear();
    }

      int HandlerRGK()
    {
        int count = 0;
        int ret_val = 1;
        byte[] reply = Data;
        string s = "";
        if (reply[32] == 1)
        {
            s = "Daily";
            count = ++Count.day;
        }
        else if (reply[32] == 2)
        {
            s = "Hourly";
            count = ++Count.hour;
        }
        else if (reply[32] == 3)
        {
            s = "Alarm";
            count = ++Count.alarm;
        }
        else if (reply[32] == 4)
        {
            s = "Audit";
            count = ++Count.audit;
        }
        WriteTrace(s + "-" + count.ToString());
        WriteTrace("<- " + DateTime.Now.ToLongTimeString() + "\t" + Gurux.Shared.GXHelpers.ToHex(reply, true));
        ProcessState = (ProcessState)reply[31];
        reply[0] = 0x69;
        reply[4] = 0;
        reply[5] = 34;
        reply[31] = 0;
        UInt16 crc = CalcCRC16(reply, 32);
        reply[32] = (byte)(crc >> 8);
        reply[33] = (byte)crc;

        Data = new byte[34];
        Array.Copy(reply, Data, Data.Length);

        IsHeader = true;
        BytesReceived = 0;
        return ret_val;
    }
    /* CRC-16, poly = x^16 + x^15 + x^2 + x^0, init = 0 */
    UInt16[] crc16_table = new UInt16[] {
        0x0000,  0x8005,  0x800f,  0x000a,  0x801b,  0x001e,  0x0014,  0x8011,
        0x8033,  0x0036,  0x003c,  0x8039,  0x0028,  0x802d,  0x8027,  0x0022,
        0x8063,  0x0066,  0x006c,  0x8069,  0x0078,  0x807d,  0x8077,  0x0072,
        0x0050,  0x8055,  0x805f,  0x005a,  0x804b,  0x004e,  0x0044,  0x8041,
        0x80c3,  0x00c6,  0x00cc,  0x80c9,  0x00d8,  0x80dd,  0x80d7,  0x00d2,
        0x00f0,  0x80f5,  0x80ff,  0x00fa,  0x80eb,  0x00ee,  0x00e4,  0x80e1,
        0x00a0,  0x80a5,  0x80af,  0x00aa,  0x80bb,  0x00be,  0x00b4,  0x80b1,
        0x8093,  0x0096,  0x009c,  0x8099,  0x0088,  0x808d,  0x8087,  0x0082,
        0x8183,  0x0186,  0x018c,  0x8189,  0x0198,  0x819d,  0x8197,  0x0192,
        0x01b0,  0x81b5,  0x81bf,  0x01ba,  0x81ab,  0x01ae,  0x01a4,  0x81a1,
        0x01e0,  0x81e5,  0x81ef,  0x01ea,  0x81fb,  0x01fe,  0x01f4,  0x81f1,
        0x81d3,  0x01d6,  0x01dc,  0x81d9,  0x01c8,  0x81cd,  0x81c7,  0x01c2,
        0x0140,  0x8145,  0x814f,  0x014a,  0x815b,  0x015e,  0x0154,  0x8151,
        0x8173,  0x0176,  0x017c,  0x8179,  0x0168,  0x816d,  0x8167,  0x0162,
        0x8123,  0x0126,  0x012c,  0x8129,  0x0138,  0x813d,  0x8137,  0x0132,
        0x0110,  0x8115,  0x811f,  0x011a,  0x810b,  0x010e,  0x0104,  0x8101,
        0x8303,  0x0306,  0x030c,  0x8309,  0x0318,  0x831d,  0x8317,  0x0312,
        0x0330,  0x8335,  0x833f,  0x033a,  0x832b,  0x032e,  0x0324,  0x8321,
        0x0360,  0x8365,  0x836f,  0x036a,  0x837b,  0x037e,  0x0374,  0x8371,
        0x8353,  0x0356,  0x035c,  0x8359,  0x0348,  0x834d,  0x8347,  0x0342,
        0x03c0,  0x83c5,  0x83cf,  0x03ca,  0x83db,  0x03de,  0x03d4,  0x83d1,
        0x83f3,  0x03f6,  0x03fc,  0x83f9,  0x03e8,  0x83ed,  0x83e7,  0x03e2,
        0x83a3,  0x03a6,  0x03ac,  0x83a9,  0x03b8,  0x83bd,  0x83b7,  0x03b2,
        0x0390,  0x8395,  0x839f,  0x039a,  0x838b,  0x038e,  0x0384,  0x8381,
        0x0280,  0x8285,  0x828f,  0x028a,  0x829b,  0x029e,  0x0294,  0x8291,
        0x82b3,  0x02b6,  0x02bc,  0x82b9,  0x02a8,  0x82ad,  0x82a7,  0x02a2,
        0x82e3,  0x02e6,  0x02ec,  0x82e9,  0x02f8,  0x82fd,  0x82f7,  0x02f2,
        0x02d0,  0x82d5,  0x82df,  0x02da,  0x82cb,  0x02ce,  0x02c4,  0x82c1,
        0x8243,  0x0246,  0x024c,  0x8249,  0x0258,  0x825d,  0x8257,  0x0252,
        0x0270,  0x8275,  0x827f,  0x027a,  0x826b,  0x026e,  0x0264,  0x8261,
        0x0220,  0x8225,  0x822f,  0x022a,  0x823b,  0x023e,  0x0234,  0x8231,
        0x8213,  0x0216,  0x021c,  0x8219,  0x0208,  0x820d,  0x8207,  0x0202
};

    UInt16 CalcCRC16(byte[] data, int len)
    {
        UInt16 crc = 0;
        UInt16 i = 0;
        while (len-- != 0)
            crc = (UInt16)((crc << 8) ^ crc16_table[(crc >> 8) ^ data[i++]]);
        return crc;
    }


    int HandlerDLMS()
    {
      int ret_val = 1;
      byte[] reply = Data;
      WriteTrace("<- " + DateTime.Now.ToLongTimeString() + "\t" + Gurux.Shared.GXHelpers.ToHex(reply, true));

      IsHeader = true;
      BytesReceived = 0;
      if (m_Client.IsDLMSPacketComplete(reply))
      {
        switch (CosemState)
        {
          case CosemState.Indication:
            //Parse reply.
            m_Client.ParseAAREResponse(reply);
            if (trace)
            {
                Console.WriteLine("Parsing AARE reply");// + BitConverter.ToString(reply) 28/03/2017
            }
            //Get challenge Is HSL authentication is used.
            if (m_Client.IsAuthenticationRequired)
            {
              foreach (byte[] it in m_Client.GetApplicationAssociationRequest())
              {
                Data = it;
                CosemState = CosemState.Authentication;
                Console.WriteLine("Authenticating", it);
              }
              m_Client.ParseApplicationAssociationResponse(reply);
            }
            else
            {
              if (trace)
              {
                Console.WriteLine("Parsing AARE reply succeeded.");
              }
              Data = m_Client.GetObjectsRequest();
              CosemState = CosemState.Relese;
              ProcessState = ProcessState.GetAvailableObjects;
            }
            break;
          
          case CosemState.Authentication:
            if (trace)
            {
              Console.WriteLine("Parsing AARE HSL reply succeeded.");
            }
           
            Data = m_Client.GetObjectsRequest();
            CosemState = CosemState.Relese;
            ProcessState = ProcessState.GetAvailableObjects;
            break;

          case CosemState.Relese:
            ReadDataBlock(reply);
            if (moredata == RequestTypes.None)
            {
              if (ProcessState == ProcessState.GetAvailableObjects)
              {
                m_Client.ParseObjects(allData, true);
                if (ClientState == ClientState.Unknown)
                {
                  Console.WriteLine("--- Available objects for PC ---");
                  foreach (GXDLMSObject it in m_Client.Objects)
                  {
                    Console.WriteLine(it.Name + " " + it.Description);
                    // todo get sap list
                    if (it is GXDLMSSapAssignment)
                    {
                      CurrentObject = it;
                      Data = ReadAttributeRequest(it, 2);
                      ProcessState = ProcessState.GetAttribute;
                    }
                  }
                }
                else
                {
                  Console.WriteLine("--- Available objects for MR ---");
                  foreach (GXDLMSObject it in m_Client.Objects)
                  {
                    Console.WriteLine(it.Name + " " + it.Description);
                  }
                  ProcessState = ProcessState.GetAttribute;
                  ObjectIndex = AttrIndex = 0;
                  CurrentObjectAttrs = GetAttributeIndex(m_Client.Objects[ObjectIndex]);
                  CurrentObject = m_Client.Objects[ObjectIndex];
                  Data = ReadAttributeRequest(CurrentObject, CurrentObjectAttrs[AttrIndex]);
                }
              }
              else if (ProcessState == ProcessState.GetAttribute)
              {
                if (ClientState == ClientState.Unknown)
                {
                  object val = ReadAttributeParse(allData, CurrentObject, 2);
                  m_Manufacturer = new GXManufacturer();
                  string[] s = new string[2];
                  s = GetIdentificator(val as List<KeyValuePair<UInt16, string>>);
                  m_Manufacturer.Identification = s[0];
                  m_Manufacturer.ServerSettings.Add(new GXServerAddress(HDLCAddressType.SerialNumber, Int32.Parse(s[1]),true) );  
                  Data = CosemInit(m_Manufacturer);
                  ProcessState = ProcessState.Idle;
                }
                else
                {
                  if (AttrIndex == 0)
                  {
                    TraceLine(logFile, "-------- Reading " + CurrentObject.GetType().Name + " " + CurrentObject.Name + " " + CurrentObject.Description);
                  }
                  object obj = ReadAttributeParse(allData, CurrentObject, CurrentObjectAttrs[AttrIndex]);
                  ViewAttribute(obj, CurrentObjectAttrs[AttrIndex]);
/*
                  if (CurrentObject.LogicalName == "7.0.94.15.2.255")
                  {
                    TableColumns = GetProfileGenericColumns(allData);
                  }
                  */
                  if (++AttrIndex == CurrentObjectAttrs.Length || CurrentObject.ObjectType == ObjectType.ProfileGeneric)
                  {
                    AttrIndex = 0;
                    if (++ObjectIndex == m_Client.Objects.Count)
                    {
                      // end reading all objects 
                      m_Device = CreateDevice();
                      Console.WriteLine("Create device and tables.");

                      ProcessState = ProcessState.GetProfileGeneric;
                      foreach (GXDLMSObject it in m_Client.Objects)
                      {
                        if (it.ObjectType == ObjectType.ProfileGeneric)
                        {
                          ObjectIndex = m_Client.Objects.IndexOf(it);
                          break;
                        }
                      }
                      CurrentObject = m_Client.Objects[ObjectIndex];
                      if (CurrentObject.LogicalName == "7.0.94.15.2.255" || CurrentObject.LogicalName == "7.0.94.15.3.255" || CurrentObject.LogicalName == "7.0.94.15.4.255")
                        Data = RequestRowsByEntry(CurrentObject, 0, 1);
                      else
                        Data = RequestRowsByRange(CurrentObject, this.GetDateTime(CurrentObject), DateTime.Now, (CurrentObject as GXDLMSProfileGeneric).CaptureObjects);

                      allData = null;
                      return ret_val;
                    }
                    else
                    {
                      CurrentObject = m_Client.Objects[ObjectIndex];
                      CurrentObjectAttrs = GetAttributeIndex(CurrentObject);
                    }
                  }
                  Data = ReadAttributeRequest(CurrentObject, CurrentObjectAttrs[AttrIndex]);
                }
              }
              else if (ProcessState == ProcessState.GetProfileGeneric)
              {
                // reading all ProfileGeneric tables data
                TraceLine(logFile, "-------- Reading " + CurrentObject.GetType().Name + " " + CurrentObject.Name + " " + CurrentObject.Description);

                if (CurrentObject.LogicalName == "7.0.94.15.2.255" || CurrentObject.LogicalName == "7.0.94.15.3.255" || CurrentObject.LogicalName == "7.0.94.15.4.255")
                {
                  ViewProfileGeneric(ParseRowsByEntry(allData, CurrentObject));
                  
                  SetExtraInfo(GetInfoColumns(allData),CurrentObject);
                  Console.WriteLine("Update columns in tables.");
                }
                else
                {
                  ViewProfileGeneric(ParseRowsByRange(allData, CurrentObject));

                  Console.WriteLine("Update rows in tables.");
                  SetTableData(allData, CurrentObject);
                  //_SetTableData(allData, CurrentObject);
                }
                if (++ObjectIndex == m_Client.Objects.Count)
                {
                  ProcessState = ProcessState.Idle;
                  CosemState = CosemState.Closing;
                  Console.WriteLine("Disconnecting from the meter.");
                  Data = m_Client.DisconnectRequest();
                }
                else
                {
                  CurrentObject = m_Client.Objects[ObjectIndex];
                  if (CurrentObject.LogicalName == "7.0.94.15.2.255" || CurrentObject.LogicalName == "7.0.94.15.3.255" || CurrentObject.LogicalName == "7.0.94.15.4.255")
                    Data = RequestRowsByEntry(CurrentObject, 0, 1);
                  else
                    Data = RequestRowsByRange(CurrentObject, this.GetDateTime(CurrentObject), DateTime.Now, (CurrentObject as GXDLMSProfileGeneric).CaptureObjects);
                }
              }
              allData = null;
            }
            break;
          
          case CosemState.Closing:
            ReadDataBlock(reply);
            if (moredata == RequestTypes.None)
            {
              if (allData[0] == 0x63)
              {
                if (sendDataFromClass != null)
                  sendDataFromClass(this, new UserEventArgs(GetAllData(Tables)));
                CosemState = CosemState.Closed;
                m_Client = null;
                ret_val = -1;
              }
            } 
            break;
        }// end switch
      }
      else
      {
        ret_val = 0;
      }
      return ret_val;
    }

    //private метод формирования таблицы
    private DataSet GetAllData(GFTables tabs)
    {
      //Создаем таблицу
      DataSet ds = new DataSet("Devices");
      DataTable tbl = null;
      tbl = new DataTable("Device");
      tbl.Columns.Add("name_enterprais");
      tbl.Rows.Add(m_Manufacturer.Identification);
      tbl.Columns.Add("device_id");
      tbl.Rows.Add(m_Client.Objects.FindByLN(ObjectType.Data,"0.0.96.1.7.255").GetValues());
      ds.Tables.Add(tbl);
      tbl = new DataTable("Tube");
      tbl.Columns.Add("flowmeter_install_site");
      tbl.Rows.Add("ГРП");
      ds.Tables.Add(tbl);
      
      foreach (Gurux.DLMS.AddIn.GXTable tab in tabs)
      {
        tbl = null;
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
      } return ds;
    }
    private DLMSDevice CreateDevice()
    {
      DLMSDevice device = new DLMSDevice(m_Client, m_Manufacturer);
      GXManufacturer man = device.Manufacturers.FindByIdentification(m_Manufacturer.Identification);
      if (man == null)
      {
        throw new Exception("Unknown manufacturer: " + m_Manufacturer.Identification);
      }
      m_Manufacturer = man;
      m_Client.ObisCodes = man.ObisCodes;
      CreateTables(device, man, m_Client);
      return device;
    }

    private void CreateTables(DLMSDevice device, GXManufacturer man, GXDLMSClient client )
    {
      //Profile generic will handle here, because it will need register objects and they must read fist.
      GXDLMSObjectCollection pg = client.Objects.GetObjects(ObjectType.ProfileGeneric);
      foreach (GXDLMSProfileGeneric it in pg)
      {
        GXTable table = new GXTable();
        table.Name = it.LogicalName + " " + GetName(man, it.ObjectType, it.LogicalName);//it.Description;
        table.ShortName = it.ShortName;
        table.LogicalName = it.LogicalName;
        table.AccessMode = AccessMode.Read;
        foreach (var it2 in it.CaptureObjects)
        {
          GXDLMSProperty prop;
          if (it2.Key is Gurux.DLMS.Objects.GXDLMSRegister)
          {
            Gurux.DLMS.Objects.GXDLMSRegister tmp = it2.Key as Gurux.DLMS.Objects.GXDLMSRegister;
            Gurux.DLMS.AddIn.GXDLMSRegister r = new Gurux.DLMS.AddIn.GXDLMSRegister();
            prop = r;
            r.Scaler = tmp.Scaler;
            r.Unit = tmp.Unit.ToString();
          }
          else if (it2.Key is Gurux.DLMS.Objects.GXDLMSClock)
          {
            prop = new GXDLMSProperty();
            prop.ValueType = typeof(DateTime);
          }
          else
          {
            prop = new GXDLMSProperty();
          }
          int index = it2.Value.AttributeIndex;
          prop.Name = it2.Key.LogicalName + " " + GetName(man, it2.Key.ObjectType, it2.Key.LogicalName);//it2.Key.Description;
          prop.ObjectType = it2.Key.ObjectType;
          prop.AttributeOrdinal = index;
          prop.LogicalName = it2.Key.LogicalName;
          
          //prop.DLMSType = it2.Value.
          //prop.ValueType = Gurux.Shared.GXHelpers.GetDataType( it2.Key.GetUIDataType(index));
          
          prop.AccessMode = AccessMode.Read;
          table.Columns.Add(prop);
        }
        Tables.Add(table);
      }
    }

    string GetName(GXManufacturer man, Gurux.DLMS.ObjectType type, string logicanName)
    {
      GXObisCode code = man.ObisCodes.FindByLN(type, logicanName, null);
      string name = null;
      if (code != null)
      {
        name = code.Description;
      }
      if (string.IsNullOrEmpty(name))
      {
        name = logicanName;
      }
      return name;
    }


    private void SetExtraInfo(Array info,GXDLMSObject item){
      GXTable table = Tables.GetTable(item);
      if (table != null)
      {
        for (int pos = 0; pos < info.Length; pos++)
        {
          if (table.Columns[pos] is Gurux.DLMS.AddIn.GXDLMSRegister)
          {
            object[] scalerUnit = (object[])info.GetValue(pos);
            ((Gurux.DLMS.AddIn.GXDLMSRegister)table.Columns[pos]).Scaler = Math.Pow(10, Convert.ToInt32(scalerUnit[0]));
            ((Gurux.DLMS.AddIn.GXDLMSRegister)table.Columns[pos]).Unit = Gurux.Shared.GXHelpers.GetUnitAsString(Convert.ToInt16(scalerUnit[1]));
          }
        }
      }
    }


    public interface IGXDLMSColumnObject {

      string SourceLogicalName{ get; set; }
      ObjectType SourceObjectType { get; set; }
      UInt16 SelectedAttributeIndex { get; set; }
      UInt16 SelectedDataIndex { get; set; }
    }

    /*
    private void SetDataInTable(byte[] ReceivedData, GXDLMSObject it)
    {
      GXTable table = m_Device.GetTable(it);
      GXDLMSProfileGeneric CurrentProfileGeneric = it as GXDLMSProfileGeneric;
      if (table != null)
      {
        Array value = (Array)m_Client.TryGetValue(ReceivedData);
        if (value != null && value.Length != 0)
        {
          object[] rows = (object[])value;
          bool LoadProfile1 = CurrentProfileGeneric.LogicalName == "0.0.99.1.0.255";
          bool LoadProfile2 = CurrentProfileGeneric.LogicalName == "0.0.99.1.2.255";
          foreach (object[] row in rows)
          {
            if (row != null)
            {
              bool skipRow = false;
              //System.Data.DataRow dr = CurrentProfileGeneric.Buffer.NewRow();
              System.Data.DataRow dr = row;
              object data = null;
              int index = 0;
              for (int pos = 0; pos < CurrentProfileGeneric.CaptureObjects.Count; ++pos)
              {
                DataType type = DataType.None;
                DataType uiType = DataType.None;
                GXDLMSObject target = CurrentProfileGeneric.CaptureObjects[pos].Key; // Abo
                IGXDLMSColumnObject target2 = target as IGXDLMSColumnObject;
                string name = null;
                ObjectType oType;
                int aIndex, dIndex;
                bool bParent = !string.IsNullOrEmpty(target2.SourceLogicalName);
                if (bParent)
                {
                  name = target2.SourceLogicalName;
                  oType = target2.SourceObjectType;
                }
                else
                {
                  name = target.LogicalName;
                  oType = target.ObjectType;
                }
                aIndex = target2.SelectedAttributeIndex;
                dIndex = target2.SelectedDataIndex;
                data = null;
                foreach (GXDLMSObject c in CurrentProfileGeneric.CaptureObjects)  // Abo
                {
                  IGXDLMSColumnObject c2 = c as IGXDLMSColumnObject;
                  if (c.ObjectType == oType && c.LogicalName == name)
                  {
                    if (aIndex == c2.SelectedAttributeIndex)
                    {
                      //Load profile is a special case.
                      if (LoadProfile1 && (index < 4) && row[index] != null)
                      {
                        data = ((Array)row[index]).GetValue(0);
                        data = GXCommon.ConvertFromDLMS(data, DataType.OctetString, DataType.DateTime, true);
                      }
                      else if (LoadProfile2)
                      {
                        if (aIndex == 1 || aIndex == 2)
                        {
                          data = ((Array)row[0]).GetValue(0);
                        }
                        else
                        {
                          data = ((Array)row[0]).GetValue(aIndex - 1);
                        }
                        if (aIndex < 3)
                        {
                          if (data != null)
                          {
                            data = ((Array)data).GetValue(index);
                            if (index == 0)
                            {
                              data = GXCommon.ConvertFromDLMS(data, DataType.OctetString, DataType.DateTime, true);
                              LastDateTime = LastDateTime.AddSeconds(1);
                            }
                          }
                          else if (index == 0)
                          {
                            skipRow = true;
                            break;
                            //LastDateTime = LastDateTime.AddMinutes(25);
                            //data = LastDateTime;
                          }
                        }
                      }
                      else
                      {
                        if (bParent && aIndex != 0 && row[index] is object[])
                        {
                          data = ((Array)((Array)row[index]).GetValue(0)).GetValue(c2.SelectedAttributeIndex);
                        }
                        else
                        {
                          data = row[index];
                        }

                        if (dIndex != 0)
                        {
                          data = ((Array)data).GetValue(dIndex - 1);
                          uiType = target.GetUIDataType(dIndex);
                          type = target.GetDataType(dIndex);
                          if (uiType == DataType.None)
                          {
                            uiType = type;
                          }
                        }
                        else
                        {
                          uiType = target.GetUIDataType(c2.SelectedAttributeIndex);
                          type = target.GetDataType(c2.SelectedAttributeIndex);
                          if (uiType == DataType.None)
                          {
                            uiType = type;
                          }
                        }
                      }
                      if (!bParent)
                      {
                        ++index;
                      }
                      break;
                    }
                  }
                  else
                  {
                    ++index;
                  }
                }
                if (skipRow)
                {
                  break;
                }
                if (data != null)
                {
                  double scaler = 1;
                  object obj = CurrentProfileGeneric.CaptureObjects[pos];
                  if (obj is Gurux.DLMS.AddIn.GXDLMSRegister)
                  {
                    scaler = ((Gurux.DLMS.AddIn.GXDLMSRegister)obj).Scaler;
                  }
                  if (obj is GXDLMSDemandRegister)
                  {
                    scaler = ((GXDLMSDemandRegister)obj).Scaler;
                  }
                  if (!data.GetType().IsArray && scaler != 1)
                  {
                    if (type == DataType.None)
                    {
                      dr[pos] = Convert.ToDouble(data) * scaler;
                    }
                    else
                    {
                      dr[pos] = Convert.ToDouble(GXCommon.ConvertFromDLMS(data, DataType.None, type, true)) * scaler;
                    }
                  }
                  else
                  {
                    dr[pos] = GXCommon.ConvertFromDLMS(data, type, uiType, true);
                  }
                }
              }
              if (!skipRow)
              {
                //CurrentProfileGeneric.Buffer.Rows.InsertAt(dr, CurrentProfileGeneric.Buffer.Rows.Count);
              }
            }
          }
        }
      }
    }

*/



    private void SetTableData(byte[] ReceivedData, GXDLMSObject it)
    {
      //Parse DLMS objects.                    
      Array reply = (Array)m_Client.GetValue(ReceivedData);
      GXTable table = Tables.GetTable(it);
      if (table != null)
      {
        // If there is no data.
        if (reply.Length == 0)
        {
          table.ClearRows();
        }
        else
        {
          int count = table.RowCount;
          List<object[]> rows = new List<object[]>(reply.Length);
          foreach (object row in reply)
          {
            rows.Add((object[])row);
          }
          table.AddRows(count, rows, false);
          //Get rows back because DeviceValueToUiValue is called.
          rows = table.GetRows(count, reply.Length, true);


          /*        Gurux.Device.Editor.IGXPartialRead partialRead = table as Gurux.Device.Editor.IGXPartialRead;
          //Save latest read time. Note we must add one second or we will read last values again.
          if (partialRead.Type == Gurux.Device.Editor.PartialReadType.New)
          {
            DateTime tm = new DateTime(2000, 1, 1);
            foreach (object[] it in rows)
            {
              if ((DateTime)it[0] > tm)
              {
                tm = (DateTime)it[0];
              }
            }
            partialRead.Start = tm.AddSeconds(1);
          }*/
        }
      }
    }

    private List<object[]> ConvertArrayToList(object[] array)
    {
      List<object[]> list = new List<object[]>();
      foreach (object[] obj in array)
      {
        list.Add(obj);
      }
      return list;
    }

    private void _SetTableData(byte[] reply, GXDLMSObject it)
    {
      object[] rows = null;
      GXTable table = Tables.GetTable(it);
      if (table != null)
      {
        // If there is no data.
        if (reply.Length == 0)
        {
          table.ClearRows();
        }
        else
        {
          int count = table.RowCount;
          GXPropertyCollection columns = table.Columns;
          rows = (object[])m_Client.GetValue(reply);  // error m_Client.ObisCodes
          if (columns != null && rows.Length != 0 && m_Client.ObisCodes.Count != 0)
          {
            Array row = (Array)rows[0];
            if (columns.Count != row.Length)
            {
              throw new Exception("Columns count do not mach.");
            }
            for (int pos = 0; pos != columns.Count; ++pos)
            {
              if (row.GetValue(pos) is byte[])
              {
                DataType type = DataType.None;
                //Find Column type
                GXObisCode code = m_Client.ObisCodes.FindByLN(columns[pos].ObjectType, columns[pos].LogicalName, null);
                if (code != null)
                {
                  GXDLMSAttributeSettings att =  code.Attributes.Find(columns[pos].AttributeOrdinal); // AttributeIndex
                  if (att != null)
                  {
                    type = att.UIType;
                  }
                }

                foreach (object[] cell in rows)
                {
                  cell[pos] = GXDLMSClient.ChangeType((byte[])cell[pos], type);
                }
              }
            }
            table.AddRows(count, ConvertArrayToList(rows), false);
            //Get rows back because DeviceValueToUiValue is called.
            rows = table.GetRows(count, rows.Length, false).ToArray();
          }
        }
      }
    }

    private void ReadDataBlock(byte[] reply)
    {
      moredata = m_Client.GetDataFromPacket(reply, ref allData);
      if (moredata == RequestTypes.None)
      {
        if (trace)
        {
          Console.WriteLine("Packet complete");
        }
      }
      else if ((moredata & RequestTypes.DataBlock) != 0)
      {
        //Send Receiver Ready.
        Data = m_Client.ReceiverReady(RequestTypes.DataBlock);
        if (trace)
        {
          Console.WriteLine("Receive next block");
        }
      }
    }

    DateTime GetDateTime(GXDLMSObject it) {

      DateTime begin;
      if (it.LogicalName == "7.0.99.99.2.255") // hourly
      {
        begin = DateTime.Now.Date.AddDays(-1).AddHours(9);
      }
      else if (it.LogicalName == "7.0.99.99.4.255") // year
      {
        begin = new DateTime(DateTime.Now.Year, 1, 1, 9, 0, 0);
      }
      else // other
      {
        begin = new DateTime(DateTime.Now.Year, DateTime.Now.Month, 1, 9, 0, 0);
      }
      return begin;
    }

    int[] GetAttributeIndex(GXDLMSObject it)
    {
      return (it as IGXDLMSBase).GetAttributeIndexToRead();
    }

    void ViewAttribute(object val, int pos)
    {
      try
      {
        //If data is array.
        if (val is byte[])
        {
          val = Gurux.Shared.GXHelpers.ToHex((byte[])val, true);
        }
        else if (val is Array)
        {
          string str = "";
          for (int pos2 = 0; pos2 != (val as Array).Length; ++pos2)
          {
            if (str != "")
            {
              str += ", ";
            }
            if ((val as Array).GetValue(pos2) is byte[])
            {
              str += Gurux.Shared.GXHelpers.ToHex((byte[])(val as Array).GetValue(pos2), true);
            }
            else
            {
              str += (val as Array).GetValue(pos2).ToString();
            }
          }
          val = str;
        }
        else if (val is System.Collections.IList)
        {
          string str = "[";
          bool empty = true;
          foreach (object it2 in val as System.Collections.IList)
          {
            if (!empty)
            {
              str += ", ";
            }
            empty = false;
            if (it2 is byte[])
            {
              str += Gurux.Shared.GXHelpers.ToHex((byte[])it2, true);
            }
            else
            {
              str += it2.ToString();
            }
          }
          str += "]";
          val = str;
        }
        TraceLine(logFile, "Index: " + pos + " Value: " + val);
      }
      catch (Exception ex)
      {
        TraceLine(logFile, "Error! Index: " + pos + " " + ex.Message);
      }
    }

    // all data ProfileGeneric in rows(row,cell)
    void ViewProfileGeneric(object[] rows)
    {
      StringBuilder sb = new StringBuilder();
      foreach (object[] row in rows)
      {
        foreach (object cell in row)
        {
          if (cell is byte[])
          {
            sb.Append(Gurux.Shared.GXHelpers.ToHex((byte[])cell, true));
          }
          else
          {
            sb.Append(Convert.ToString(cell));
          }
          sb.Append(" | ");
        }
        sb.Append("\r\n");
      }
      Trace(logFile, sb.ToString());
    }

    #region RECEIVE
    public void Receive(SocketAsyncEventArgs e)
    {
      if (Socket.ProtocolType == ProtocolType.Tcp)
      {
        e.SetBuffer(Header, 0, HeaderLengthDLMS);
        bool willRaiseEvent = Socket.ReceiveAsync(e);
        if (!willRaiseEvent)
        {
          ProcessReceive(e);
        }
      }
      else if (Socket.ProtocolType == ProtocolType.Udp)
      {
        Data = new byte[1024];
        e.SetBuffer(Data, 0, Data.Length);
        bool willRaiseEvent = Socket.ReceiveFromAsync(e);
        if (!willRaiseEvent)
        {
          ProcessReceive(e);
        }
      }
    }
    public bool ProcessReceive(SocketAsyncEventArgs e)
    {
      bool ret_val = false;
      if (e.SocketError != SocketError.Success)
      {
        return ret_val;
      }
      // check if the remote host closed the connection
      if (e.BytesTransferred > 0)
      {
        LastActive = Environment.TickCount;
        //increment the count of the total bytes receive by the server
        Interlocked.Add(ref BytesReceived, e.BytesTransferred);
        if (Socket.ProtocolType == ProtocolType.Tcp)
        {
          if (trace)
          {
            Console.WriteLine("The server has read a total of {0} bytes", BytesReceived);
          }
          if (IsHeader)
          {
            if (e.Offset + e.BytesTransferred < HeaderLengthDLMS)
            {
              e.SetBuffer(Header, e.BytesTransferred, HeaderLengthDLMS - e.BytesTransferred);
              bool willRaiseEvent = Socket.ReceiveAsync(e);
              if (!willRaiseEvent)
              {
                ProcessReceive(e);
              }
            }
            else
            {
                //  if we've filled the buffer we can decode the header
                if (trace)
                {
                  Console.WriteLine("Receive header");
                }
                IsHeader = false;
                //DataLength = (UInt16)(Header[4] << 8 | Header[5]); for RGK
                DataLength = (UInt16)(Header[6] << 8 | Header[7]);
                DataLength += Convert.ToUInt16(e.BytesTransferred + e.Offset);
                Data = new byte[DataLength];
                Array.Copy(Header, Data, e.BytesTransferred + e.Offset);
                e.SetBuffer(Data, e.BytesTransferred + e.Offset, Data.Length - (e.BytesTransferred + e.Offset));
              
                bool willRaiseEvent = Socket.ReceiveAsync(e);
                if (!willRaiseEvent)
                {
                  ProcessReceive(e);
                }
              }
             
          }
          else
          {
            if (trace)
            {
              Console.WriteLine("Receive packet. There are offset {0} bytes, BytesTransferred {1} bytes",
                  e.Offset, e.BytesTransferred);
            }

            if (BytesReceived == DataLength)
            {
              if (trace)
              {
                Console.WriteLine("Receive packet. There are {0} bytes in packet",
                    DataLength);
              }
              //  process the message
              if (HandlerDLMS() > 0)
              {
                Send(e);
              }
              else
              {
                ret_val = true;
              }
            }
            else
            {
              e.SetBuffer(Data, BytesReceived, Data.Length - BytesReceived);
              bool willRaiseEvent = Socket.ReceiveAsync(e);
              if (!willRaiseEvent)
              {
                ProcessReceive(e);
              }
            }
          }
        }
        else if (Socket.ProtocolType == ProtocolType.Udp)
        {

          DataLength = (UInt16)(Data[6] << 8 | Data[7]);

          if (trace)
          {
            Console.WriteLine("Receive packet. There are offset {0} bytes, BytesTransferred {1} bytes",
                e.Offset, e.BytesTransferred);
          }

          if (e.BytesTransferred == DataLength + 8)
          {
            if (trace)
            {
              Console.WriteLine("Receive packet. There are {0} bytes in packet",
                  DataLength);
            }
            // Remove any extra data
            if (e.BytesTransferred < Data.Length)
            {
              Array.Resize<Byte>(ref Data, e.BytesTransferred);
            }

            //  process the message
            if (HandlerDLMS() > 0)
            {
              Send(e);
            }
            else
            {
              ret_val = true;
            }
          }
        }
      }
      else
      {
        ret_val = true;
      }
      return ret_val;
    }
    #endregion
/**
    #region RECEIVE_UDP
    public bool ProcessReceiveUdp(SocketAsyncEventArgs e)
    {
      bool ret_val = false;
      if (e.SocketError != SocketError.Success)
      {
        return ret_val;
      }
      // check if the remote host closed the connection
      if (e.BytesTransferred > 0)
      {
        LastActive = Environment.TickCount;
        //increment the count of the total bytes receive by the server
        Interlocked.Add(ref BytesReceived, e.BytesTransferred);
            
        DataLength = (UInt16)(Data[6] << 8 | Data[7]);
   
          if (trace)
          {
            Console.WriteLine("Receive packet. There are offset {0} bytes, BytesTransferred {1} bytes",
                e.Offset, e.BytesTransferred);
          }

          if (e.BytesTransferred == DataLength+8)
          {
            if (trace)
            {
              Console.WriteLine("Receive packet. There are {0} bytes in packet",
                  DataLength);
            }
            // Remove any extra data
            if (e.BytesTransferred < Data.Length)
            {
              Array.Resize<Byte>(ref Data, e.BytesTransferred);
            }        

            //  process the message
            int state = Handler();
            if (state > 0)
            {
              StartSend(e);
            }
            else
            {
              ret_val = true;
            }
          }
      }
      else
      {
        ret_val = true;
      }
      return ret_val;
    }
    #endregion
   **/ 
    #region SEND
    public void Send(SocketAsyncEventArgs e)
    {
      WriteTrace("-> " + DateTime.Now.ToLongTimeString() + "\t" + Gurux.Shared.GXHelpers.ToHex(Data, true));
      byte[] buf = new byte[Data.Length];
      Array.Copy(Data, buf, buf.Length);
      e.SetBuffer(buf, 0, buf.Length);

      //post asynchronous send operation
      if (Socket.ProtocolType == ProtocolType.Tcp)
      {
        bool willRaiseEvent = Socket.SendAsync(e);
        if (!willRaiseEvent)
        {
          ProcessSent(e);
        }
      }
      else if (Socket.ProtocolType == ProtocolType.Udp)
      {
        bool willRaiseEvent = Socket.SendToAsync(e);
        if (!willRaiseEvent)
        {
          ProcessSent(e);
        }
      }
    }
    public bool ProcessSent(SocketAsyncEventArgs e)
    {
      bool ret_val = false;
      if (e.SocketError == SocketError.Success)
      {
        // If we are within this if-statement, then all the bytes in
        // the message have been sent.    
          Receive(e);
      }
      else
      {
        ret_val = true;
      }
      return ret_val;
    }
    #endregion
    
    string[] GetIdentificator(List<KeyValuePair<UInt16, string>> sap)
    {
      List<string> list = new List<string>();
      foreach (var it in sap)
      {
        list.Add(it.Value);
      }

      // Remainder of string starting at ','.
      string sn = list[0];
      char[] ret_val = new char[3];
      sn.CopyTo(0, ret_val, 0, 3);
      string[] s = new string[2];
      s[0] = new string(ret_val);
      ret_val = new char[8];
      sn.CopyTo(8, ret_val, 0, 8);
      s[1] = new string(ret_val);
      return s;
    }

    private void UpdateManufactureSettings(string id)
    {
      m_Client.InterfaceType = InterfaceType.Net;
      if (id != null)
      {
        if (m_Manufacturer != null && string.Compare(m_Manufacturer.Identification, id, true) != 0)
        {
          throw new Exception(string.Format("m_Manufacturer type does not match. m_Manufacturer is {0} and it should be {1}.", id, m_Manufacturer.Identification));
        }
        m_Client.Authentications = m_Manufacturer.Settings;
        m_Client.UseLogicalNameReferencing = m_Manufacturer.UseLogicalNameReferencing;

        //If is manufacturer supporting IEC 62056-47
        if (m_Manufacturer.UseIEC47)
        {
          m_Client.ClientID = Convert.ToUInt16(m_Manufacturer.GetAuthentication(m_Client.Authentication).ClientID);
          m_Client.ServerID = Convert.ToUInt16(m_Manufacturer.ServerSettings[0].PhysicalAddress);
        }
      }
      else
      {
        m_Client.UseLogicalNameReferencing = true;
        m_Client.ClientID = 16;
        m_Client.ServerID = 1;
      }
    }

    private byte[] CosemInit(GXManufacturer man)
    {
      m_Manufacturer = man;
      if (man != null)
      {
        m_Client.Authentication = Authentication.Low;
        m_Client.Password = Encoding.ASCII.GetBytes(m_Manufacturer.Settings[(int)m_Client.Authentication].Password); 
        UpdateManufactureSettings(man.Identification);
        ClientState = ClientState.Identification;
      }
      else
      {
        UpdateManufactureSettings(null);
      }
      foreach (byte[] it in m_Client.AARQRequest(null))
      {
        if (trace)
        {
          Console.WriteLine("Send AARQ request", BitConverter.ToString(it));
        }
        CosemState = CosemState.Indication;
        return it;
      }
      return null;
    }

    /// <summary>
    /// Read attribute value.
    /// </summary>
    private byte[] ReadAttributeRequest(GXDLMSObject it, int attributeIndex)
    {
      //return m_Client.Read(it.Name, it.ObjectType, attributeIndex)[0];
      return m_Client.Read(it, attributeIndex)[0];

    }
    private object ReadAttributeParse(byte[] reply, GXDLMSObject it, int attributeIndex)
    {
      //Update data type.
      if (it.GetDataType(attributeIndex) == DataType.None)
      {
        it.SetDataType(attributeIndex, m_Client.GetDLMSDataType(reply));
      }
      return m_Client.UpdateValue(reply, it, attributeIndex);
    }

    /// <summary>
    /// Write attribute value.
    /// </summary>
    byte[] WriteRequest(GXDLMSObject it, int attributeIndex)
    {
      return m_Client.Write(it, attributeIndex)[0];
    }

    /// <summary>
    /// Method attribute value.
    /// </summary>
    byte[] MethodRequest(GXDLMSObject it, int attributeIndex, object value, DataType type)
    {
      return m_Client.Method(it, attributeIndex, value, type)[0];
    }

    /// <summary>
    /// Request Profile Generic Columns by entry.
    /// </summary>
    byte[] RequestRowsByEntry(GXDLMSObject it, int index, int count)
    {
      return m_Client.ReadRowsByEntry(it.Name, index, count);
    }
    
    /// <summary>
    /// Response Profile Generic Columns by entry.
    /// </summary>
    object[] ParseRowsByEntry(byte[] reply, GXDLMSObject it)
    {
      return (object[])m_Client.UpdateValue(reply, it, 2);
    }

   
    /// <summary>
    /// Request Profile Generic Columns by range.
    /// </summary>
    byte[] RequestRowsByRange(GXDLMSObject it, DateTime start, DateTime end, List<GXKeyValuePair<GXDLMSObject, GXDLMSCaptureObject>> columns)
    {
      GXDLMSObject col = columns[0].Key;
      return m_Client.ReadRowsByRange(it.Name, col.LogicalName, col.ObjectType, col.Version, start, end);
    }

    /// <summary>
    /// Parse Profile Generic Columns by range.
    /// </summary>
    object[] ParseRowsByRange(byte[] reply, GXDLMSObject it)
    {
      return (object[])m_Client.UpdateValue(reply, it, 2);
    }


    public List<GXKeyValuePair<GXDLMSObject, GXDLMSCaptureObject>> GetProfileGenericColumns(byte[] data)
    {
      return m_Client.ParseColumns(data);
    }

    public Array GetInfoColumns(byte[] data)
    {
      // Get columns header value
      object[] values = (object[])m_Client.GetValue(data);
      return values[0] as Array;
    }


    /// <summary>
    /// Read data type of selected attribute index.
    /// </summary>
    byte[] GetDLMSDataTypeRequest(GXDLMSObject it, int attributeIndex)
    {
      return m_Client.Read(it.Name, it.ObjectType, attributeIndex)[0];
    }
    
    /// <summary>
    /// Read data type of selected attribute index.
    /// </summary>
    DataType GetDLMSDataTypeResponse(byte[] reply)
    {
      return m_Client.GetDLMSDataType(reply);
    }

    void WriteTrace(string line)
    {
      if (trace)
      {
        Console.WriteLine(line);
      }
      using (TextWriter writer = new StreamWriter(File.Open("trace.txt", FileMode.Append)))
      {
        writer.WriteLine(line);
      }
    }

    #region IDisposable Members

    /// <summary>
    /// Release instance.
    /// </summary>
    public void Dispose()
    {
      if (this.Socket != null)
      {
        if (this.Socket.Connected)
        {
          try
          {
            this.Socket.Shutdown(SocketShutdown.Send);
            Console.WriteLine(DateTime.Now.ToLongTimeString() +"\t" +"Socket shutdown");
          }
          catch (Exception)
          {
            // Throw if client has closed, so it is not necessary to catch.
              Console.WriteLine(DateTime.Now.ToLongTimeString() + "\t" + "Socket Exception");
          }
          finally
          {
            this.Socket.Close();
            Console.WriteLine(DateTime.Now.ToLongTimeString() + "\t" + "Socket close");
          }
        }
        this.Socket = null;
      }
      
        //logFile.Flush();
        logFile.Close();
    }

    #endregion

    #region LOGFILE
    static void Trace(TextWriter writer, string text)
    {
      writer.Write(text);
      Console.Write(text);
    }

    static void TraceLine(TextWriter writer, string text)
    {
      writer.WriteLine(text);
      Console.WriteLine(text);
    }

    #endregion

  }
}
