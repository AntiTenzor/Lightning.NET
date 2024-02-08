using System;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Diagnostics;
using System.Globalization;
using System.Collections.Generic;

using LightningDB;

namespace TestReader05
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
                env.Open(EnvironmentOpenFlags.ReadOnly);

                /*

                using (var tx = env.BeginTransaction(TransactionBeginFlags.ReadOnly))
                using (var db = tx.OpenDatabase(
                    name: StringKeyDbName,
                    closeOnDispose: true,
                    configuration: new DatabaseConfiguration { Flags = DatabaseOpenFlags.IntegerKey }))
                {
                    // !!!WARNING!!! This loop BLOCKS WRITER!
                    // Probably, this is because I do not close transaction.
                    while (true)
                    {
                        Console.WriteLine("Iterating over DB with WHILE loop...");
                        using (LightningCursor curs = tx.CreateCursor(db))
                        {
                            while (curs.Next() == MDBResultCode.Success)
                            {
                                var (c_resultCode, c_key, c_value) = curs.GetCurrent();
                                c_resultCode.ThrowOnError();

                                string tkey = String.Join(", ", c_key.CopyToNewArray());
                                string tval = Encoding.UTF8.GetString(c_value.CopyToNewArray());
                                Console.WriteLine($"  key:[ {tkey} ] ==> {tval}");
                            }
                        }
                        Console.WriteLine();

                        Thread.Sleep(1_000);
                    }
                }
                Console.WriteLine();

                */



                while (true)
                {
                    using (var tx = env.BeginTransaction(TransactionBeginFlags.ReadOnly))
                    using (var db = tx.OpenDatabase(
                        name: StringKeyDbName,
                        closeOnDispose: true,
                        configuration: new DatabaseConfiguration { Flags = DatabaseOpenFlags.IntegerKey }))
                    {
                        Console.WriteLine("Iterating over DB with WHILE loop...");
                        using (LightningCursor curs = tx.CreateCursor(db))
                        {
                            while (curs.Next() == MDBResultCode.Success)
                            {
                                var (c_resultCode, c_key, c_value) = curs.GetCurrent();
                                c_resultCode.ThrowOnError();

                                var ks = from k in c_key.CopyToNewArray() select k.ToString().PadLeft(3);
                                string tkey = String.Join(", ", ks);
                                string tval = Encoding.UTF8.GetString(c_value.CopyToNewArray());
                                Console.WriteLine($"  key:[ {tkey} ] ==> {tval}");
                            }
                        }
                    }
                    Console.WriteLine();

                    Thread.Sleep(1_000);
                } // End while (true)
            }

            Console.WriteLine("Press ENTER to finish...");
            Console.ReadLine();
        }
    }
}
