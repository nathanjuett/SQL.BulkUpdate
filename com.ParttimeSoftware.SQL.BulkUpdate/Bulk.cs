using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Concurrent;
using System.Data.SqlClient;
using com.ParttimeSoftware.SQL.BulkUpdate.EF;

namespace com.ParttimeSoftware.SQL.BulkUpdate
{


    public class Bulk<T> : IDisposable
        where T : class
    {
        ManualResetEvent Wait = null;
        ManualResetEvent WaitDrain = null;
        int Batch = 1000;
        int BatchDrain = 10000;
        ConcurrentQueue<T> Queue = new ConcurrentQueue<T>();
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
        public void AddToQueue(T item)
        {
            Queue.Enqueue(item);
        }
        public void Complete()
        {
            Wait.Set();
            Completed = true;
            WaitDrain.WaitOne();

        }
        public static Bulk<T> BulkInsert()
        {
            return new Bulk<T>();
        }
        public static Bulk<T> BulkInsertOnly()
        {
            Bulk<T> ret = new Bulk<T>();
            ret.InsertOnly = true;
            return ret;
        }
        public void Process()
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
            }
            WaitDrain.Set();
        }

        private void Upload(int batch)
        {
            List<T> tmplst = new List<T>();
            for (int i = 0; i <= batch; i++)
            {
                T tmp;
                if (Queue.TryDequeue(out tmp))
                {
                    tmplst.Add(tmp);
                }
            }
            if (InsertOnly)
            {
                //Thread InsertThread = new Thread(new ParameterizedThreadStart(InsertToDB));
                //InsertThread.Start(tmplst);
                InsertToDB(tmplst);
            }
            else
            {
                UpdateDB(tmplst);
            }
        }

        private void UpdateDB(List<T> tmplst)
        {
            throw new NotImplementedException();
        }

        private void InsertToDB(object tmplst)
        {
            List<T> lst = (List<T>)tmplst;
            Context db = new Context();
            SqlBulkCopy BulkCopy = new SqlBulkCopy(db.Database.Connection.ConnectionString);
            BulkCopy.DestinationTableName = db.GetTableName<T>();
            BulkCopy.BatchSize = 100;
            foreach (var item in typeof(T).GetProperties())
            {
                BulkCopy.ColumnMappings.Add(new SqlBulkCopyColumnMapping(item.Name, item.Name));
            }
            BulkCopy.WriteToServer(GenericListDataReaderExtensions.GetDataReader<T>(lst));
         }

        #region IDisposable Support
        private bool disposedValue = false; // To detect redundant calls

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    Complete();
                    Exit = true;
                    if (UploadThread.ThreadState == ThreadState.Running)
                        UploadThread.Abort();
                }

                // TODO: free unmanaged resources (unmanaged objects) and override a finalizer below.
                // TODO: set large fields to null.

                disposedValue = true;
            }
        }

        // TODO: override a finalizer only if Dispose(bool disposing) above has code to free unmanaged resources. 
        // ~Bulk() {
        //   // Do not change this code. Put cleanup code in Dispose(bool disposing) above.
        //   Dispose(false);
        // }

        // This code added to correctly implement the disposable pattern.
        public void Dispose()
        {
            // Do not change this code. Put cleanup code in Dispose(bool disposing) above.
            Dispose(true);
            // TODO: tell GC not to call its finalizer when the above finalizer is overridden.
            // GC.SuppressFinalize(this);
        }
        #endregion
    }
}
