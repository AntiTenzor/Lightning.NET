using System;
using System.Text;
using System.Collections.Generic;



namespace LightningDB;

public static class LightningEnvironmentExtentions
{
    /// <summary>
    /// This method packs all magic to the single call:
    /// - Begin transaction;
    /// - Open database;
    /// - Put;
    /// - Commit;
    /// 
    /// It throws an exception in all cases except Success
    /// </summary>
    /// <param name="env">existing and OPEN environment</param>
    /// <param name="dbName">database name (equivalent of table name in relational DB)</param>
    /// <param name="key">integer key</param>
    /// <param name="text">string value to put to the DB (NOT NULL!)</param>
    /// <param name="encoding">encoding to convert string to byte array</param>
    /// <param name="transBeginFlags">transaction flags (default: None)</param>
    /// <param name="dbFlags">database flags (default: Create + IntegerKey)</param>
    /// <param name="putOptions">special put options (default: None)</param>
    /// <returns>in case of any problems this will throw an exception, so return value is always Success</returns>
    public static MDBResultCode Put(this LightningEnvironment env,
        string dbName,
        int key, string text, Encoding encoding,
        TransactionBeginFlags transBeginFlags = TransactionBeginFlags.None,
        DatabaseOpenFlags dbFlags = DatabaseOpenFlags.Create | DatabaseOpenFlags.IntegerKey,
        PutOptions putOptions = PutOptions.None)
    {
        using (var tx = env.BeginTransaction(beginFlags: transBeginFlags))
        using (var db = tx.OpenDatabase(
                    name: dbName,
                    closeOnDispose: true,
                    configuration: new DatabaseConfiguration { Flags = dbFlags }))
        {
            MDBResultCode res1 = tx.Put(db, key, text, encoding, options: putOptions);
            res1.ThrowOnError();

            // Commit closes transaction
            MDBResultCode res2 = tx.Commit();
            res2.ThrowOnError();
        }

        return MDBResultCode.Success;
    }

    /// <summary>
    /// This method packs all magic to the single call:
    /// - Begin transaction;
    /// - Open database;
    /// - TryGet;
    /// 
    /// It throws an exception in critical cases
    /// </summary>
    /// <param name="env">existing and OPEN environment</param>
    /// <param name="dbName">database name (equivalent of table name in relational DB)</param>
    /// <param name="key">integer key</param>
    /// <param name="encoding">Encoding to convert byte array to string (NOT NULL!).</param>
    /// <param name="text">A string containing the value found in the database, if it exists.</param>
    /// <param name="transBeginFlags">transaction flags (default: ReadOnly)</param>
    /// <param name="dbFlags">database flags (default: IntegerKey)</param>
    /// <returns>true, in case of success; false if key not found; throws an exception in case of critical errors</returns>
    public static bool TryGet(this LightningEnvironment env,
        string dbName,
        int key, Encoding encoding, out string text,
        TransactionBeginFlags transBeginFlags = TransactionBeginFlags.ReadOnly,
        DatabaseOpenFlags dbFlags = DatabaseOpenFlags.IntegerKey)
    {
        using (var tx = env.BeginTransaction(beginFlags: transBeginFlags))
        using (var db = tx.OpenDatabase(
                    name: dbName,
                    closeOnDispose: true,
                    configuration: new DatabaseConfiguration { Flags = dbFlags }))
        {
            bool ok = tx.TryGet(db, key, encoding, out text);
            return ok;
        }
    }
}
