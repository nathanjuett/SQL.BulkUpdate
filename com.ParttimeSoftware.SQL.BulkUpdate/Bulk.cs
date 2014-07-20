using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Concurrent;
using System.Data.SqlClient;
using com.ParttimeSoftware.SQL.BulkUpdate;
using System.Data.Entity;
using System.Data.Entity.Core.Objects;
using System.Data.Entity.Infrastructure;
using System.Text.RegularExpressions;

namespace com.ParttimeSoftware.SQL.BulkUpdate
{
    public static class EFHelper
    {
      public static string GetTableName<T>(this DbContext context) where T : class
    {
        ObjectContext objectContext = ((IObjectContextAdapter)context).ObjectContext;

        return objectContext.GetTableName<T>();
    }

    public static string GetTableName<T>(this ObjectContext context) where T : class
    {

        string sql = context.CreateObjectSet<T>().ToTraceString();
        Regex regex = new Regex(@"FROM\s+(?<table>.+)\s+AS");
        Match match = regex.Match(sql);

        string table = match.Groups["table"].Value;
        return table;
    }
}



    public class Bulk<Tcontext, Tclass> : IDisposable
        where Tcontext : DbContext, new()
        where Tclass : class
    {
        ManualResetEvent Wait = null;
        ManualResetEvent WaitDrain = null;
        int Batch = 1000;
        int BatchDrain = 10000;
        ConcurrentQueue<Tclass> Queue = new ConcurrentQueue<Tclass>();
        Thread UploadThread;
        bool Exit = false;
        bool InsertOnly = false;
        bool Completed = false;
        private Bulk()
        {
            Wait = new ManualResetEvent(false);
            WaitDrain = new ManualResetEvent(false);
            UploadThread = new Thread(new ThreadStart(Process));
            UploadThread.Start();
        }
        public void AddToQueue(Tclass item)
        {
            Queue.Enqueue(item);
        }
        private void Complete()
        {
            Wait.Set();
            Completed = true;
            WaitDrain.WaitOne();
        }
        public static Bulk<Tcontext, Tclass> BulkInsert()
        {
            return new Bulk<Tcontext, Tclass>();
        }
        public static Bulk<Tcontext, Tclass> BulkInsertOnly()
        {
            Bulk<Tcontext, Tclass> ret = new Bulk<Tcontext, Tclass>();
            ret.InsertOnly = true;
            return ret;
        }
        private void Process()
        {
            while (!Queue.IsEmpty)
            {
                if (Queue.Count > Batch)
                {
                    Upload(Batch);
                }
                else if (Completed)
                {
                    Upload(BatchDrain);
                }
                Thread.Sleep(100);
                if (Exit)
                    return;
            }
            WaitDrain.Set();
        }

        private void Upload(int batch)
        {
            if (InsertOnly)
            {
                Thread InsertThread = new Thread(new ParameterizedThreadStart(InsertToDB));
                InsertThread.Start(batch);
            }
            else
                UpdateDB(batch);
        }

        private List<Tclass> GetUploadListfromQueue(int batch)
        {
            List<Tclass> tmplst = new List<Tclass>();
            for (int i = 0; i <= batch; i++)
            {
                Tclass tmp;
                if (Queue.TryDequeue(out tmp))
                    tmplst.Add(tmp);
            }
            return tmplst;
        }

        private void UpdateDB(int tmplst)
        {
            //Context db = new Context();
            //var exits = db.Database.SqlQuery(typeof(int), string.Format("select count(*) from sys.types where name = {0} ", "utt_" + typeof(T).Name));



            throw new NotImplementedException();
        }

        private void InsertToDB(object batch)
        {
            Tcontext db = new Tcontext();
            SqlBulkCopy BulkCopy = new SqlBulkCopy(db.Database.Connection.ConnectionString);
            BulkCopy.DestinationTableName = db.GetTableName<Tclass>();
            BulkCopy.BatchSize = 100;
            foreach (var item in typeof(Tclass).GetProperties())
                BulkCopy.ColumnMappings.Add(new SqlBulkCopyColumnMapping(item.Name, item.Name));
            BulkCopy.WriteToServer(GenericListDataReaderExtensions.GetDataReader<Tclass>(GetUploadListfromQueue((int)batch)));
        }
        public void Abort()
        {
            Exit = true;
            Wait.Set();
            WaitDrain.Set();
        }
        #region IDisposable Support
        private bool disposedValue = false;

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    Complete();
                    if (UploadThread.ThreadState == ThreadState.Running)
                        UploadThread.Abort();
                }
                Queue = null;
                disposedValue = true;
            }
        }

        public void Dispose()
        {
            Dispose(true);
        }
        #endregion
    }
}
