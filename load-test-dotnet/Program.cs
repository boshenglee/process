using Cassandra;
using System.Diagnostics;
using System.Threading;


public class Program
{
    private static readonly string[] scLogs = new string[]
    {
        "[954821740] PCT2075: TEMPERATURE : MB[42.37 C] PMS[44.25 C]",
        "[954821740] MSEQ_V3: Motor speeds non-zero on arrival: A=11 B=6 C=14 D=11 : Initiating force stop of all motors.",
        "[954821740] MSEQ_V3: | SCurve:16 SA:11 SB:6 SC:14 SD:11 | CA:-332 CB:-109 CC:289 CD:-238 | EA:0 EB:0 DA:0.00 DB:0.00 | EST_D:1590.51",
        "[954821740] MSEQ_V3: Motor speeds non-zero on arrival: A=6 B=2 C=4 D=7 : Initiating force stop of all motors.",
        "[954821740] MSEQ_V3: ARRIVAL CONFIRMED - Cube: 2, Axis: MCUBE_Y, Dir: MCUBE_FORWARD",
        "[954821740] MSEQ_V3: Estimated Payload: Total-[PAYLOAD_BIN_10KG] A-[7][PAYLOAD_BIN_10KG][CURR_A: 19.5100A] B-[11][PAYLOAD_NONE][CURR_B: 15.1700A]",
        "[954821740] MSEQ_V3: MOTOR A: ANGLE:1396 | DISTANCE: 2010.10 mm | ESTIMATE D:1593.27 mm | TARGET D:2020.00 mm | ERROR D:-416.82 mm | ENCA:0.00 mm ENCB:0.00 mm | CUBE:2",
        "[954821740] MSEQ_V3: MOTOR B: ANGLE:1388 | DISTANCE: 1998.58 mm | ESTIMATE D:1593.27 mm | TARGET D:2020.00 mm | ERROR D:-405.30 mm | ENCA:0.00 mm ENCB:0.00 mm | CUBE:2",
        "[954821740] MSEQ_V3: MOTOR C: ANGLE:1384 | DISTANCE: 1992.82 mm | ESTIMATE D:1593.27 mm | TARGET D:2020.00 mm | ERROR D:-399.54 mm | ENCA:0.00 mm ENCB:0.00 mm | CUBE:2",
        "[954821740] MSEQ_V3: MOTOR D: ANGLE:1382 | DISTANCE: 1989.94 mm | ESTIMATE D:1593.27 mm | TARGET D:2020.00 mm | ERROR D:-396.66 mm | ENCA:0.00 mm ENCB:0.00 mm | CUBE:2",
        "[954821740] MCUBE_SEQ: [CNT:4] MCUBE ID PAIR Y [8][3][7][4]",
        "[954821740] MCUBE_SEQ: MOTOR STATUS CHECK [(0)ID:8,498V,32C,0] [(0)ID:3,498V,31C,0] [(0)ID:7,504V,31C,0] [(0)ID:4,497V,31C,0]",
        "[954821740] WEX_SEQ: MOTOR STATUS CHECK [(0)ID:11,502V,31C,0] [(0)ID:12,502V,29C,0]",
        "[954821740] WEX_SEQ: WEX_CMD_STATUS2: [A:11][SPEED:0][TEMP:31][IQ:-31][ENC:65473]",
        "[954821740] WEX_SEQ: WEX_CMD_STATUS2: [B:12][SPEED:0][TEMP:29][IQ:-36][ENC:65473]",
        "[954821740] MASTER_SEQ: RIGHT : 1, LEFT : 1, FRONT : 1, BACK : 1",
        "[954821740] MASTER_SEQ: CURRENTLY ALIGN AT XY",
        "[954821740] WEX_SEQ_V2: WEX CALIBRATION MODE POS",
        "[954821740] WEX_SEQ_V2: WEX A [x:0][y:1][xy:0][xyc:0]",
        "[954821740] WEX_SEQ_V2: WEX B [x:0][y:1][xy:0][xyc:0]",
        "[954821740] WEX_SEQ_V2: WEX CURRENTLY AT Y",
        "[954821740] WEX_SEQ_V2: WEX [xy] CURRENTLY AT Y, GOING TO XY [ANGLE:-10]",
        "[954821740] WEX_SEQ_V2: WEX [xy] ENCODER START POSITION [ENCODER A:6399] [ENCODER B:6399]"
    };

    private static readonly string[] DB = new string[]
    {
        "skycar_logs_without_index",
        "skycar_logs_with_index",
        "skycar_logs_with_partition"
    };

    private static int lines = 10;
    private static string dbName = DB[0];
    private static Cassandra.ISession session;

    public static void Main(string[] args)
    {
        var host = CreateHostBuilder(args).Build();

        ConnectToCassandra();
        host.Run();
    }

    public static void ConnectToCassandra()
    {
        var cluster = Cluster.Builder()
                     .AddContactPoints("cassandra")
                     .WithAuthProvider(new PlainTextAuthProvider("cassandra", "cassandra"))
                     .Build();

        session = cluster.Connect("skycar_logs");
    }
    
    public class SkycarLogs
    {
        public Guid uuid { get; set; }
        public int skycarID { get; set; }
        public string message { get; set; }
        public long createAt { get; set; }

        public SkycarLogs()
        {
            Random random = new Random();
            this.uuid = Guid.NewGuid();
            this.skycarID = random.Next(10)+1;
            this.message = scLogs[random.Next(0, scLogs.Length)];
            this.createAt = (long)(DateTime.UtcNow - new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc)).TotalMilliseconds;
        }       
    }

// Create an object with constructor

    public static void WriteToCassandra(SkycarLogs log)
    {
        session.Execute(new SimpleStatement(
            $"INSERT INTO {dbName} (id, sc_id, message, created_at) VALUES (?,?, ?,?)",
            log.uuid,log.skycarID,log.message,log.createAt)
            );

    }

    public static int ReadAllFromCassandra()
    {
        var rs = session.Execute($"SELECT * FROM {dbName}");
        var rowCount = 0;
        foreach (var row in rs)
        {
            rowCount++;

        // Do something with the value
        }
        return rowCount;
    }
    
    public static double QueryFromCassandra(int limit)
    {
        Random random = new Random();
        var skycarID = random.Next(10)+1;

        DateTime currentUtcTime = DateTime.UtcNow;
        long toTime = (long)(currentUtcTime - new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc)).TotalMilliseconds;

        DateTime tenMinutesBefore = currentUtcTime.AddMinutes(-10);
        long fromTime = (long)(tenMinutesBefore - new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc)).TotalMilliseconds;

        var queryStopWatch  = Stopwatch.StartNew();
        var rs = session.Execute(
            $"SELECT sc_id,message,created_at FROM {dbName} " +
            $"WHERE sc_id = {skycarID} AND created_at >= {fromTime} and CREATED_AT <= {toTime} "+
            $"LIMIT {limit} ");
        queryStopWatch.Stop();
        var elapsedTime = queryStopWatch.Elapsed.TotalMilliseconds;
        Console.WriteLine($"Query logs in {elapsedTime} ms");
        return elapsedTime;
    }
    
    public static double QueryFromCassandraNopartition(int limit)
    {
        Random random = new Random();
        var skycarID = random.Next(10)+1;

        DateTime currentUtcTime = DateTime.UtcNow;
        long toTime = (long)(currentUtcTime - new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc)).TotalMilliseconds;

        DateTime tenMinutesBefore = currentUtcTime.AddMinutes(-10);
        long fromTime = (long)(tenMinutesBefore - new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc)).TotalMilliseconds;

        var queryStopWatch  = Stopwatch.StartNew();
        var rs = session.Execute(
            $"SELECT sc_id,message,created_at FROM {dbName} " +
            $"WHERE sc_id = {skycarID} AND created_at >= {fromTime} and CREATED_AT <= {toTime} "+
            $"LIMIT {limit} ALLOW FILTERING");
        queryStopWatch.Stop();

        var elapsedTime = queryStopWatch.Elapsed.TotalMilliseconds;
        Console.WriteLine($"Query logs in {elapsedTime} ms");
        return elapsedTime;
    }

    public static void Run()
    {
        var overallStopWatch = Stopwatch.StartNew();
        for (int i = 0; i < lines; i++)
        {
            SkycarLogs log = new SkycarLogs();
            var singlelStopWatch = Stopwatch.StartNew();
            WriteToCassandra(log);
            singlelStopWatch.Stop();
            Console.WriteLine($"INSERTING ({i+1}): {singlelStopWatch.Elapsed.TotalMilliseconds} milliseconds  DB: {dbName}");


        }
        overallStopWatch.Stop();
        Console.WriteLine($"Inserted {lines} logs in {overallStopWatch.Elapsed.TotalMilliseconds} ms");

    }
    public static IHostBuilder CreateHostBuilder(string[] args) =>
        Microsoft.Extensions.Hosting.Host.CreateDefaultBuilder(args)
            .ConfigureWebHostDefaults(webBuilder =>
            {
                webBuilder.UseUrls("http://0.0.0.0:4000");

                webBuilder.ConfigureServices(services => { })
                    .Configure(app =>
                    {
                        app.UseRouting();

                        app.UseEndpoints(endpoints =>
                        {
                            endpoints.MapPost("/change-lines", async context =>
                            {
                                if (int.TryParse(context.Request.Query["lines"], out int newLines))
                                {
                                    lines = newLines;
                                }

                                await context.Response.WriteAsJsonAsync(new { lines });
                            });

                            endpoints.MapPost("/change-db", async context =>
                            {
                                if (int.TryParse(context.Request.Query["db"], out int dbIndex) && dbIndex >= 0 && dbIndex < DB.Length)
                                {
                                    dbName = DB[dbIndex];
                                }

                                await context.Response.WriteAsync(dbName);
                            });

                            endpoints.MapGet("/start", context =>
                            {
                                var t = new Thread(() => Run());
                                t.Start();

                                return context.Response.WriteAsync(dbName);
                            });

                            endpoints.MapGet("/skycar-logs", async context =>
                            {
                                var numOfRecord = ReadAllFromCassandra();
                                await context.Response.WriteAsJsonAsync(new { numOfRecord });
                            });

                            endpoints.MapGet("/skycar-logs-with-filter", async context =>
                            {
                                if (int.TryParse(context.Request.Query["limit"], out int limit))
                                {
                                    var elapsedTime = QueryFromCassandra(limit);
                                    await context.Response.WriteAsJsonAsync(new { times = elapsedTime });
                                }
                            });

                            endpoints.MapGet("/skycar-logs-with-filter-no-partition", async context =>
                            {
                                if (int.TryParse(context.Request.Query["limit"], out int limit))
                                {
                                    var elapsedTime = QueryFromCassandraNopartition(limit);
                                    await context.Response.WriteAsJsonAsync(new { times = elapsedTime });
                                }
                            });
                        });
                    });
            });
}