using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.Entity.Core;
using System.Data.Entity;
using System.Data.Entity.Core.Objects;
using System.Text.RegularExpressions;
using System.Data.Entity.Infrastructure;

namespace com.ParttimeSoftware.SQL.BulkUpdate.EF
{
    public class Context : DbContext
    {
        public DbSet<User> User { get; set; }
        public Context()
        {
            
        }
    }
    
}
