using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using com.ParttimeSoftware.SQL.BulkUpdate;
using System.Linq;
using System.Collections.Generic;

namespace com.ParttimeSoftware.SQL.BulkUpdate.Test
{
    [TestClass]
    public class UpdateTests
    {
        [TestInitialize()]
        public void InitTest()
        {
            EF.Context db = new EF.Context();
            db.Database.ExecuteSqlCommand("truncate table users");

        }
        [TestCleanup]
        public void CleanupTest()
        {
            EF.Context db = new EF.Context();
            db.Database.ExecuteSqlCommand("truncate table users");

        }
        [TestMethod]
        public void TestAddUser()
        {
            Guid value = Guid.NewGuid();
            EF.Context db = new EF.Context();
            db.User.Add(new EF.User() { Name = value.ToString(), Value = value.ToString() });
            db.SaveChanges();

            db = new EF.Context();
            EF.User user = db.User.Where(o => o.Name == value.ToString()).Single();
            Assert.IsTrue(user != null);
            Assert.AreEqual(value.ToString(), user.Value);
            db.User.Remove(user);
            db.SaveChanges();
        }
        [TestMethod]
        public void TestInsert()
        {
            List<EF.User> users = new List<EF.User>();

            using (Bulk<EF.Context, EF.User> bulkuser = Bulk<EF.Context, EF.User>.BulkInsertOnly())
            {
                for (int i = 0; i < 1000; i++)
                {
                    Guid value = Guid.NewGuid();
                    bulkuser.AddToQueue(new EF.User() { Name = value.ToString(), Value = value.ToString() });
                }
            }

            EF.Context db = new EF.Context();
            Assert.AreEqual(1000, db.User.Count());
        }
        [TestMethod]
        public void TestInsertThenAbort()
        {
            List<EF.User> users = new List<EF.User>();
            using (Bulk<EF.Context, EF.User> bulkuser = Bulk<EF.Context, EF.User>.BulkInsertOnly())
            {
                for (int i = 0; i < 100000; i++)
                {
                    Guid value = Guid.NewGuid();
                    bulkuser.AddToQueue(new EF.User() { Name = value.ToString(), Value = value.ToString() });
                }
                bulkuser.Abort();
            }

            EF.Context db = new EF.Context();
            Assert.IsTrue(100000 > db.User.Count());
 
        }
    }
}
