using System;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Diagnostics;
using System.Globalization;
using System.Collections.Generic;

using LightningDB;

namespace TestReader07
{
    internal class Program
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
                env.Open(EnvironmentOpenFlags.ReadOnly);

                int key = 0;
                long counter = 0;
                bool ok = false;
                string text = "";
                Stopwatch sw = Stopwatch.StartNew();
                while (true)
                {
                    using (var tx = env.BeginTransaction(TransactionBeginFlags.ReadOnly))
                    using (var db = tx.OpenDatabase(
                        name: StringKeyDbName,
                        closeOnDispose: true,
                        configuration: new DatabaseConfiguration { Flags = DatabaseOpenFlags.IntegerKey }))
                    {
                        ok = tx.TryGet(db, key, Encoding.UTF8, out text);
                    }

                    counter++;

                    if (counter % 1_000_000 == 0)
                    {
                        TimeSpan ts = sw.Elapsed;
                        double speedPerSec = counter / ts.TotalSeconds;

                        Console.WriteLine("Key: {0}; counter: {1}; elapsed: {2}; speed: {3} read/sec;   text: {4}",
                            key, counter, ts, speedPerSec, text);

                        key = (key + 1) % 20;
                    }
                } // End while (true)
            }

            Console.WriteLine("Press ENTER to finish...");
            Console.ReadLine();
        }
    }
}
