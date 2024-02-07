using System;

namespace LightningDB;

public sealed class Utils
{
    #region Buffer manipulations
    public static unsafe Int32 ReadInt32(byte[] buf, int offset)
    {
        fixed (byte* pData = &buf[offset])
        {
            Int32 res = *((Int32*)pData);
            return res;
        }
    }

    public static unsafe Int64 ReadInt64(byte[] buf, int offset)
    {
        fixed (byte* pData = &buf[offset])
        {
            Int64 res = *((Int64*)pData);
            return res;
        }
    }

    public static unsafe void WriteInt32(Int32 val, byte[] buf, int offset)
    {
        fixed (byte* pData = &buf[offset])
        {
            Int32* ptr = (Int32*)pData;
            *ptr = val;
        }
    }

    public static unsafe void WriteInt64(Int64 val, byte[] buf, int offset)
    {
        fixed (byte* pData = &buf[offset])
        {
            Int64* ptr = (Int64*)pData;
            *ptr = val;
        }
    }
    #endregion Buffer manipulations
}
