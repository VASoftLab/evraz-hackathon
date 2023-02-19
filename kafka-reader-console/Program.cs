using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.NetworkInformation;
using System.Net;
using System.Text;
using System.Threading.Tasks;

using System.Data.SQLite;
using Newtonsoft.Json;

using Confluent.Kafka;
using static System.Net.WebRequestMethods;

// https://cloud.yandex.ru/docs/managed-kafka/operations/connect
// https://kafka.apache.org/documentation/#consumerconfigs

namespace kafka_reader_console
{
    public class Program
    {
        static void Main(string[] args)
        {
            // string HOST = "rc1a-b5e65f36lm3an1d5.mdb.yandexcloud.net:9091";
            string HOST = "rc1a-2ar1hqnl386tvq7k.mdb.yandexcloud.net:9091";
            string TOPIC = "zsmk-9433-dev-01";
            string USER = "9433_reader";
            string PASS = "eUIpgWu0PWTJaTrjhjQD3.hoyhntiK";

            string KEY;
            string VALUE;

            // string dbName = "D:\\SourceCode\\evraz-hackathon\\hackathon.db";
            string dbName = "hackathon.db";

            var consumerConfig = new ConsumerConfig(
                new Dictionary<string, string>{
                    {"bootstrap.servers", HOST},
                    {"security.protocol", "SASL_SSL"},
                    {"sasl.mechanisms", "SCRAM-SHA-512"},
                    {"sasl.username", USER},
                    {"sasl.password", PASS},
                    {"ssl.ca.location", "certificate.pem"},
                    {"auto.offset.reset", "earliest"},
                    //{"enable.auto.commit", "false"},
                    {"group.id", "vasoftlab"}
                }
            );

            var consumer = new ConsumerBuilder<string, string>(consumerConfig).Build();
            consumer.Subscribe(TOPIC);
            try
            {
                while (true)
                {
                    var cr = consumer.Consume();
                    Console.WriteLine($"{cr.Message.Key}:{cr.Message.Value}");

                    if (!String.IsNullOrEmpty(cr.Message.Key))
                        KEY = cr.Message.Key.Trim();
                    else
                        KEY = String.Empty;

                    if (!String.IsNullOrEmpty(cr.Message.Value))
                        VALUE = cr.Message.Value.Trim();
                    else
                        VALUE = String.Empty;

                    //dynamic obj = JsonConvert.DeserializeObject(VALUE);
                    //var moment = obj["SM_Exgauster\\[0:0]"];

                    using (var conn = new SQLiteConnection($"Data Source = '{dbName}';Version=3;"))
                    {
                        conn.Open();
                        string sql = "INSERT INTO tblData (timestamp, key, value) VALUES (@pTimestamp, @pKey, @pValue)";

                        using (var cmd = new SQLiteCommand(sql, conn))
                        {
                            try
                            {
                                cmd.Parameters.AddWithValue("@pTimestamp", DateTimeToUnixTimeStamp(DateTime.Now));
                                cmd.Parameters.AddWithValue("@pKey", KEY);
                                cmd.Parameters.AddWithValue("@pValue", VALUE);
                                cmd.ExecuteNonQuery();
                            }
                            catch (Exception ex)
                            {
                                conn.Close();
                            }
                        }
                        if (conn.State == System.Data.ConnectionState.Open)
                        { conn.Close(); }
                    }
                }
            }
            catch (OperationCanceledException)
            {
                // Ctrl-C was pressed.
            }
            finally
            {
                consumer.Close();
            }
        }

        private static DateTime UnixTimeStampToDateTime(double unixTimeStamp)
        {
            System.DateTime dtDateTime = new DateTime(1970, 1, 1, 0, 0, 0, 0, System.DateTimeKind.Local);
            dtDateTime = dtDateTime.AddSeconds(unixTimeStamp).ToLocalTime();
            return dtDateTime;
        }
        private static double DateTimeToUnixTimeStamp(DateTime dt)
        {
            return (dt.Subtract(new DateTime(1970, 1, 1, 0, 0, 0, 0, System.DateTimeKind.Local))).TotalSeconds;
        }
    }
}
