using System;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Diagnostics;
using System.Globalization;
using System.Collections.Generic;

using LightningDB;

namespace TestWriter03
{
    public class Program
    {
        public const string StringKeyDbName = "IntKey3";

        static void Main(string[] args)
        {
            EnvironmentConfiguration envConf = new EnvironmentConfiguration();
            envConf.MaxDatabases = 4;

            Console.WriteLine("  Max readers: {0}", envConf.MaxReaders);

            using (var env = new LightningEnvironment("C:\\tmp\\TestLMDB\\", envConf))
            {
                // Other modifiers are too strange to use them
                env.Open(EnvironmentOpenFlags.None);

                using (var tx = env.BeginTransaction())
                using (var db = tx.OpenDatabase(
                    name: StringKeyDbName,
                    closeOnDispose: true,
                    configuration: new DatabaseConfiguration { Flags = DatabaseOpenFlags.Create | DatabaseOpenFlags.IntegerKey }))
                {
                    MDBResultCode res = tx.TruncateDatabase(db);
                    Console.WriteLine("Result of TruncateDatabase: {0}", res);
                    Console.WriteLine();
                    // Commit closes transaction and makes it unusable
                    tx.Commit();
                }



                Console.WriteLine("Let's write to Database once per second.");
                Console.WriteLine("I will commit every write.");
                Console.WriteLine();

                int counter = 0;
                while (true)
                {
                    using (var tx = env.BeginTransaction())
                    using (var db = tx.OpenDatabase(
                        name: StringKeyDbName,
                        closeOnDispose: true,
                        configuration: new DatabaseConfiguration { Flags = DatabaseOpenFlags.Create | DatabaseOpenFlags.IntegerKey }))
                    {
                        int key = counter % 20;
                        string ks = key.ToString().PadLeft(3);
                        MDBResultCode res1 = tx.Put(db,
                            key, Encoding.UTF8.GetBytes($"String value for integer key {ks}. Counter: {counter}"));
                        // Commit closes transaction
                        MDBResultCode res2 = tx.Commit();

                        Console.WriteLine("Result of Put+Commit for INTEGER key {0} counter {1}: {2} + {3}", key, counter, res1, res2);
                    }

                    counter++;

                    Thread.Sleep(1_000);
                } // End while (true)
            } // End using (var env = new LightningEnvironment("C:\\tmp\\TestLMDB\\", envConf))


            Console.WriteLine("Press ENTER to finish...");
            Console.ReadLine();
        }
    }
}
