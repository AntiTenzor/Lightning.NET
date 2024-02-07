using System;
using System.IO;
using System.Linq;
using System.Text;
using System.Diagnostics;
using System.Globalization;
using System.Collections.Generic;

using LightningDB;

namespace TestWriter01
{
    public class Program
    {
        public const string StringKeyDbName = "StringKey";

        static void Main(string[] args)
        {
            const string keyHello = "hello";

            EnvironmentConfiguration envConf = new EnvironmentConfiguration();
            envConf.MaxDatabases = 4;

            Console.WriteLine("  Max readers: {0}", envConf.MaxReaders);

            using (var env = new LightningEnvironment("C:\\tmp\\TestLMDB\\", envConf))
            {
                // Другие модификаторы слишком мутные, чтобы их использовать
                env.Open(EnvironmentOpenFlags.None);

                using (var tx = env.BeginTransaction())
                using (var db = tx.OpenDatabase(
                    name: StringKeyDbName,
                    closeOnDispose: true,
                    configuration: new DatabaseConfiguration { Flags = DatabaseOpenFlags.Create }))
                {
                    MDBResultCode res = tx.TruncateDatabase(db);
                    Console.WriteLine("Result of TruncateDatabase: {0}", res);
                    Console.WriteLine();
                    // Коммит закрывает транзакцию???
                    //tx.Commit();



                    string val = "world";

                    res = tx.Put(db,
                        Encoding.UTF8.GetBytes(keyHello), Encoding.UTF8.GetBytes(val));
                    Console.WriteLine("Result of 1st Put: {0}", res);

                    // Коммит закрывает транзакцию???
                    //tx.Commit();

                    res = tx.Put(db,
                        Encoding.UTF8.GetBytes(keyHello), Encoding.UTF8.GetBytes(val));
                    // BadCommand -- нельзя делать Update ??? -- Транзакция после комита возвращает BadCommand
                    // Коммит закрывает транзакцию!!!
                    Console.WriteLine("Result of 2nd Put: {0}", res);

                    //tx.Commit();

                    bool ok = tx.TryGet(db,
                        Encoding.UTF8.GetBytes(keyHello), out byte[] getVal);
                    if (ok)
                        Console.WriteLine("Result of 1st Get: {0}", Encoding.UTF8.GetString(getVal));
                    else
                        Console.WriteLine("НЕ СМОГ ПОЛУЧИТЬ ЗНАЧЕНИЕ ПО ЕГО КЛЮЧУ!");

                    res = tx.Delete(db,
                        Encoding.UTF8.GetBytes(""));
                    // BadValSize -- Нельзя использовать ключ нулевой длины!
                    Console.WriteLine("Result of Delete with empty key: {0}", res);

                    //tx.Commit();

                    res = tx.Delete(db,
                        Encoding.UTF8.GetBytes("~~~ N/A ~~~"));
                    // NotFound -- ожидаемо не получается найти значение для отсутствующего ключа =)
                    Console.WriteLine("Result of Delete with wrong key: {0}", res);

                    //tx.Commit();

                    res = tx.Put(db,
                        new byte[0], Encoding.UTF8.GetBytes(val));
                    // BadValSize -- Нельзя использовать ключ нулевой длины!
                    Console.WriteLine("Result of Put for EMPTY key: {0}", res);

                    tx.Commit();
                    Console.WriteLine("Result of tx.Commit()      : {0}", res);
                }
                Console.WriteLine();

                using (var tx = env.BeginTransaction(TransactionBeginFlags.ReadOnly))
                using (var db = tx.OpenDatabase(name: StringKeyDbName))
                {
                    var (resultCode, key, value) = tx.Get(db, Encoding.UTF8.GetBytes(keyHello));
                    // Success -- ожидаемо работает получение элемента по существующему ключу
                    Console.WriteLine("Result of 2nd Get: {0}", resultCode);
                    Console.WriteLine($"  key:{Encoding.UTF8.GetString(key.CopyToNewArray())} ==> {Encoding.UTF8.GetString(value.CopyToNewArray())}");

                    Console.WriteLine();

                    Console.WriteLine("Iterating over DB...");
                    using (var curs = tx.CreateCursor(db))
                    {
                        foreach (var kvp in curs.AsEnumerable())
                        {
                            string tkey = Encoding.UTF8.GetString(kvp.Item1.CopyToNewArray());
                            string tval = Encoding.UTF8.GetString(kvp.Item2.CopyToNewArray());
                            Console.WriteLine($"  key:{tkey} ==> {tval}");
                        }
                    }
                }
                Console.WriteLine();

                const long amount = 1_000_000;
                Stopwatch sw = Stopwatch.StartNew();
                {
                    using (MemoryStream keyMs = new MemoryStream())
                    using (MemoryStream valMs = new MemoryStream())
                    using (BinaryWriter keyBw = new BinaryWriter(keyMs, Encoding.ASCII))
                    using (BinaryWriter valBw = new BinaryWriter(keyMs, Encoding.ASCII))
                    {
                        for (long j = 0; j < amount; j++)
                        {
                            //int key = (int)(j % 10_000L);
                            string key = (j % 1_000L).ToString();
                            DateTime val = DateTime.UtcNow;

                            keyBw.Seek(0, SeekOrigin.Begin);
                            keyBw.Write(key);

                            valBw.Seek(0, SeekOrigin.Begin);
                            valBw.Write(val.ToString("o", CultureInfo.InvariantCulture));

                            using (var tx = env.BeginTransaction())
                            using (var db = tx.OpenDatabase(
                                name: StringKeyDbName,
                                closeOnDispose: true,
                                configuration: new DatabaseConfiguration { Flags = DatabaseOpenFlags.Create }))
                            {
                                MDBResultCode res = tx.Put(db,
                                    keyMs.GetBuffer(), valMs.GetBuffer());
                                if (res != MDBResultCode.Success)
                                {
                                    Console.WriteLine("  BAD Result of {0}th Put: {1}", j, res);
                                    Console.ReadLine();
                                }

                                tx.Commit();
                            }
                        }
                    }
                }
                sw.Stop();
                double speedPerSec = amount / sw.Elapsed.TotalSeconds;
                // В базу вставлено 1_000_000 значений за 10620327,1 микросекунд. speedPerSec: 94159,0584342737
                Console.WriteLine("В базу вставлено {0} значений за {1} микросекунд. speedPerSec: {2}",
                    amount, sw.ElapsedTicks / 10.0, speedPerSec);
                Console.WriteLine();

                MDBResultCode flushRes = env.Flush(true);
                Console.WriteLine("Result of 1st env.Flush: {0}", flushRes);

                Stats stats = env.EnvironmentStats;
                // Количество баз данных -- ОДНА ШТУКА =)
                Console.WriteLine("Environment entries: {0}", stats.Entries);
                Console.WriteLine();



            } // using (var env = new LightningEnvironment("C:\\tmp\\TestLMDB\\", envConf))

            Console.WriteLine("Press ENTER to finish...");
            Console.ReadLine();
        }
    }
}
