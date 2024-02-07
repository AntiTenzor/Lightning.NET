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
            // Байтовое представление числа 7
            byte[] keyForNull = new byte[] { 7, 0, 0, 0 };

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
                    // BadValSize -- Key must be an array of strictly positive length!
                    Console.WriteLine("Result of Delete with empty key: {0}", res);




                    res = tx.Delete(db, null);
                    // BadValSize -- Нельзя использовать ключ NULL!
                    Console.WriteLine("Result of Delete with NULL key : {0}", res);



                    res = tx.Delete(db,
                        Encoding.UTF8.GetBytes("~~~ N/A ~~~"));
                    // NotFound -- ожидаемо не получается найти значение для отсутствующего ключа =)
                    Console.WriteLine("Result of Delete with wrong key: {0}", res);



                    res = tx.Put(db,
                        new byte[0], Encoding.UTF8.GetBytes(val));
                    // BadValSize -- Key must be an array of strictly positive length!
                    Console.WriteLine("Result of Put for EMPTY key    : {0}", res);



                    res = tx.Put(db, keyForNull, null);
                    // К сожалению, значение NULL превратится в массив нулевой длины?
                    Console.WriteLine("Result of Put for NULL value   : {0}", res);

                    ok = tx.TryGet(db, keyForNull, out byte[] getNull);
                    if (ok)
                        Console.WriteLine("Result of Get(keyForNull) IS NULL? {0}", (getNull == null));
                    else
                        Console.WriteLine("НЕ СМОГ ПОЛУЧИТЬ ЗНАЧЕНИЕ ПО ЕГО КЛЮЧУ keyForNull!");



                    res = tx.Put(db, 9, Encoding.UTF8.GetBytes("String value for integer key 9. Is it OK?"));
                    Console.WriteLine("Result of Put for INTEGER key 9: {0}", res);



                    res = tx.Put(db, 3, Encoding.UTF8.GetBytes("Value with integer key 3 now should be located before keys 7 and 9. Is it OK?"));
                    Console.WriteLine("Result of Put for INTEGER key 3: {0}", res);


                    if (tx.TryGet(db, 3, out byte[] getBy3))
                        Console.WriteLine("Result of TryGet(3)            : {0}", Encoding.UTF8.GetString(getBy3));
                    else
                        Console.WriteLine("TryGet(3) returned FALSE.");


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

                    Console.WriteLine("Iterating over DB with FOREACH loop...");
                    using (var curs = tx.CreateCursor(db))
                    {
                        // .AsEnumerable() is quite slow method
                        foreach (var kvp in curs.AsEnumerable())
                        {
                            string tkey = String.Join(", ", kvp.Item1.CopyToNewArray());
                            string tval = Encoding.UTF8.GetString(kvp.Item2.CopyToNewArray());
                            Console.WriteLine($"  key:[ {tkey} ] ==> {tval}");
                        }
                    }
                    Console.WriteLine();


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
                }
                Console.WriteLine();



            } // End using (var env = new LightningEnvironment("C:\\tmp\\TestLMDB\\", envConf))

            Console.WriteLine("Press ENTER to finish...");
            Console.ReadLine();
        }
    }
}
