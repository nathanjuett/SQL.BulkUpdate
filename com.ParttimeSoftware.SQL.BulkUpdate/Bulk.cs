using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Concurrent;

namespace com.ParttimeSoftware.SQL.BulkUpdate
{
    

    public class Bulk<T> : IDisposable
    {
        ManualResetEvent Wait = null;
        WaitHandle WaitDrain = null;
        int Batch = 1000;
        int BatchDrain = 10000;
        ConcurrentQueue<T> Queue = new ConcurrentQueue<T>();
        Thread UploadThread;
        bool Exit = false;
        bool InsertOnly = false;
        private Bulk()
        {
            Wait = new ManualResetEvent(false);
            UploadThread = new Thread(new ThreadStart(Process));
            UploadThread.Start();
        }
        public Bulk<T> BulkInsert()
        {
             return new Bulk<T>();
        }
        public Bulk<T> BulkInsertOnly()
        {
            Bulk<T> ret = new Bulk<T>();
            ret.InsertOnly = true;
            return ret;
        }
        public void Process()
        {
            while (!Queue.IsEmpty && !Exit)
            {
                if (Queue.Count > Batch)
                {
                    Upload(Batch);
                }
                else if (Exit)
                {
                    Upload(BatchDrain);
                }
                Thread.Sleep(100);
            }
            WaitDrain.WaitOne();
        }

        private void Upload(int batch)
        {
            List<T> tmplst = new List<T>();
            for (int i = 0; i == batch; i++)
            {
                T tmp;
                if (Queue.TryDequeue(out tmp))
                {
                    tmplst.Add(tmp);
                }
            }
            if (InsertOnly)
            {
                Thread InsertThread = new Thread(new ParameterizedThreadStart(InsertToDB));
                InsertThread.Start(tmplst);
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

            throw new NotImplementedException();
        }
              
        #region IDisposable Support
        private bool disposedValue = false; // To detect redundant calls

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                         Exit = true;
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
 