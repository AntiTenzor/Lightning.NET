using System;
using System.IO;
using System.Linq;
using System.Text;
using System.Diagnostics;
using System.Globalization;
using System.Collections.Generic;

using LightningDB;

namespace TestWriter03
{
    public class Program
    {
        public const string StringKeyDbName = "IntKey";

        static void Main(string[] args)
        {
            // Байтовое представление числа 0
            byte[] keyHello = new byte[4];

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
                    configuration: new DatabaseConfiguration { Flags = DatabaseOpenFlags.Create | DatabaseOpenFlags.IntegerKey }))
                {
                    MDBResultCode res = tx.TruncateDatabase(db);
                    Console.WriteLine("Result of TruncateDatabase: {0}", res);
                    Console.WriteLine();
                    // Коммит закрывает транзакцию???
                    //tx.Commit();



                    string val = "world";

                    res = tx.Put(db,
                        keyHello, Encoding.UTF8.GetBytes(val), PutOptions.None);
                    Console.WriteLine("Result of 1st Put: {0}", res);



                    res = tx.Put(db,
                        keyHello, Encoding.UTF8.GetBytes(val));
                    // BadCommand -- нельзя делать Update ??? -- Транзакция после комита возвращает BadCommand
                    // Коммит закрывает транзакцию!!!
                    Console.WriteLine("Result of 2nd Put: {0}", res);



                    bool ok = tx.TryGet(db,
                        keyHello, out byte[] getVal);
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



                    res = tx.Put(db,
                        new byte[0], Encoding.UTF8.GetBytes(val));
                    // BadValSize -- Нельзя использовать ключ нулевой длины!
                    Console.WriteLine("Result of Put for EMPTY key    : {0}", res);




                    // Коммит закрывает транзакцию
                    res = tx.Commit();
                    Console.WriteLine("Result of tx.Commit()          : {0}", res);
                }
                Console.WriteLine();



                using (var tx = env.BeginTransaction(TransactionBeginFlags.ReadOnly))
                using (var db = tx.OpenDatabase(
                    name: StringKeyDbName,
                    closeOnDispose: true,
                    configuration: new DatabaseConfiguration { Flags = DatabaseOpenFlags.IntegerKey }))
                {
                    var (resultCode, key, value) = tx.Get(db, keyHello);
                    // Success -- ожидаемо работает получение элемента по существующему ключу
                    Console.WriteLine("Result of 2nd Get: {0}", resultCode);
                    Console.WriteLine($"  key:[ {String.Join(", ", key.CopyToNewArray())} ] ==> {Encoding.UTF8.GetString(value.CopyToNewArray())}");

                    Console.WriteLine();

                    Console.WriteLine("Iterating over DB...");
                    using (var curs = tx.CreateCursor(db))
                    {
                        foreach (var kvp in curs.AsEnumerable())
                        {
                            string tkey = String.Join(", ", kvp.Item1.CopyToNewArray());
                            string tval = Encoding.UTF8.GetString(kvp.Item2.CopyToNewArray());
                            Console.WriteLine($"  key:[ {tkey} ] ==> {tval}");
                        }
                    }
                }
                Console.WriteLine();



            } // End using (var env = new LightningEnvironment("C:\\tmp\\TestLMDB\\", envConf))

            Console.WriteLine("Press ENTER to finish...");
            Console.ReadLine();
        }
    }
}
