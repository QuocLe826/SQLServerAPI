using System.Data;
using System.Data.SqlClient;

namespace SQLServerAPI
{
    public class DatabaseProvider
    {
        /// <summary>
        /// Chuỗi kết nối đến database SQL Server
        /// </summary>
        public  string ConnectionString { get; set; }

        /// <summary>
        /// Biến chứa dữ liệu kết nối đến SQL Server
        /// </summary>
        public SqlConnection DbConnection { get; set; }

        /// <summary>
        /// Biến chứa transaction của Database kết nối
        /// </summary>
        public  SqlTransaction DbTransaction { get; set; }

        /// <summary>
        /// Thiết lập dữ liệu connection string
        /// </summary>
        /// <param name="dataSource"></param>
        /// <param name="initialCatalog"></param>
        /// <param name="userId"></param>
        /// <param name="password"></param>
        public  void SetConnectionData(string dataSource, string initialCatalog, string userId, string password)
        {
            ConnectionString = string.Format(@"Data Source={0}; Initial Catalog={1}; User ID={2};Password={3}",
                dataSource.Trim(), initialCatalog.Trim(), userId.Trim(), password.Trim());
        }

        /// <summary>
        /// Mở kết nối database 
        /// </summary>
        /// <returns></returns>
        public SqlConnection OpenConnection()
        {
            DbConnection = new SqlConnection(ConnectionString);
            if (DbConnection.State == ConnectionState.Closed || DbConnection.State == ConnectionState.Broken)
            {
                DbConnection.Open();
            }
            return DbConnection;
        }

        /// <summary>
        /// ExecuteNonQuery
        /// </summary>
        /// <param name="query"></param>
        /// <param name="commandType"></param>
        /// <param name="parameters"></param>
        /// <returns></returns>
        public int ExecuteNonQuery(string query, CommandType commandType = CommandType.Text, params SqlParameter[] parameters)
        {
            if (DbConnection != null && !string.IsNullOrEmpty(DbConnection.ConnectionString) && DbConnection.State != ConnectionState.Closed)
            {
                using (var command = DbConnection.CreateCommand())
                {
                    command.CommandTimeout = 60000;
                    command.CommandText = query;
                    command.Transaction = DbTransaction;
                    command.CommandType = commandType;
                    command.Parameters.AddRange(parameters);
                    return command.ExecuteNonQuery();
                }
            }
            using (var conn = new SqlConnection(ConnectionString))
            {
                if (conn.State == ConnectionState.Closed)
                {
                    conn.Open();
                }
                var command = conn.CreateCommand();
                command.CommandTimeout = 60000;
                command.CommandText = query;
                command.CommandType = commandType;
                command.Parameters.AddRange(parameters);
                return command.ExecuteNonQuery();
            }
        }

        /// <summary>
        /// ExecuteScalar
        /// </summary>
        /// <param name="query"></param>
        /// <param name="commandType"></param>
        /// <param name="parameters"></param>
        /// <returns></returns>
        public object ExecuteScalar(string query, CommandType commandType = CommandType.Text, params SqlParameter[] parameters)
        {
            if (DbConnection != null && !string.IsNullOrEmpty(DbConnection.ConnectionString) && DbConnection.State != ConnectionState.Closed)
            {
                using (var command = DbConnection.CreateCommand())
                {
                    command.CommandTimeout = 60000;
                    command.CommandText = query;
                    command.Transaction = DbTransaction;
                    command.CommandType = commandType;
                    command.Parameters.AddRange(parameters);
                    return command.ExecuteScalar();
                }
            }
            using (var conn = new SqlConnection(ConnectionString))
            {
                if (conn.State == ConnectionState.Closed)
                {
                    conn.Open();
                }
                var command = conn.CreateCommand();
                command.CommandTimeout = 60000;
                command.CommandText = query;
                command.CommandType = commandType;
                command.Parameters.AddRange(parameters);
                return command.ExecuteScalar();
            }
        }

        /// <summary>
        /// ExecuteDataTable
        /// </summary>
        /// <param name="query"></param>
        /// <param name="commandType"></param>
        /// <param name="parameters"></param>
        /// <returns></returns>
        public DataTable ExecuteDataTable(string query, CommandType commandType = CommandType.Text, params SqlParameter[] parameters)
        {
            var dtSet = new DataTable();
            if (DbConnection != null && !string.IsNullOrEmpty(DbConnection.ConnectionString) && DbConnection.State != ConnectionState.Closed)
            {
                using (SqlCommand command = DbConnection.CreateCommand())
                {
                    command.CommandTimeout = 60000;
                    command.CommandText = query;
                    command.Transaction = DbTransaction;
                    command.CommandType = commandType;
                    command.Parameters.AddRange(parameters);
                    var adapter = new SqlDataAdapter(command);
                    adapter.Fill(dtSet);
                    command.Parameters.Clear();
                    return dtSet;
                }
            }
            using (var conn = new SqlConnection(ConnectionString))
            {
                if (conn.State == ConnectionState.Closed)
                {
                    conn.Open();
                }
                using (SqlCommand command = conn.CreateCommand())
                {
                    command.CommandTimeout = 60000;
                    command.CommandText = query;
                    command.CommandType = commandType;
                    command.Parameters.AddRange(parameters);
                    var adapter = new SqlDataAdapter(command);
                    adapter.Fill(dtSet);
                    command.Parameters.Clear();
                }
            }
            return dtSet;
        }

        /// <summary>
        /// Kiểm tra database đã tồn tại
        /// </summary>
        /// <param name="databaseName"></param>
        /// <returns></returns>
        public bool CheckDatabaseExists(string databaseName)
        {
            var query = string.Format(@"IF EXISTS (SELECT 1 FROM master.dbo.sysdatabases WHERE NAME = '{0}')
                            begin
	                            select 1
                            end", databaseName);
            var result = ExecuteDataTable(query);
            return result != null && result.Rows.Count > 0;
        }

        /// <summary>
        /// Kiểm tra kết nối đến SQL Server
        /// </summary>
        /// <param name="dataSource"></param>
        /// <param name="initialCatalog"></param>
        /// <returns></returns>
        public bool CheckConnection(string dataSource, string initialCatalog)
        {
            try
            {
                var connectionString = string.Format(@"Data Source={0}; Initial Catalog={1}",
                    dataSource.Trim(), initialCatalog.Trim());
                var dbConnection = new SqlConnection(connectionString);
                if (dbConnection.State == ConnectionState.Closed || dbConnection.State == ConnectionState.Broken)
                {
                    dbConnection.Open();
                }
                return true;
            }
            catch
            {
                return false;
            }
        }

        /// <summary>
        /// Kiểm tra kết nối đến SQL Server
        /// </summary>
        /// <param name="dataSource"></param>
        /// <param name="initialCatalog"></param>
        /// <param name="userId"></param>
        /// <param name="password"></param>
        /// <returns></returns>
        public bool CheckConnection(string dataSource, string initialCatalog, string userId, string password)
        {
            try
            {
                var connectionString = string.Format(@"Data Source={0}; Initial Catalog={1}; User ID={2};Password={3}",
                    dataSource.Trim(), initialCatalog.Trim(), userId.Trim(), password.Trim());
                var dbConnection = new SqlConnection(connectionString);
                if (dbConnection.State == ConnectionState.Closed || dbConnection.State == ConnectionState.Broken)
                {
                    dbConnection.Open();
                }
                return true;
            }
            catch
            {
                return false;
            }
        }

        /// <summary>
        /// Kiểm tra kết nối đến SQL Server
        /// </summary>
        /// <param name="connectionString"></param>
        /// <returns></returns>
        public bool CheckConnection(string connectionString)
        {
            try
            {
                var dbConnection = new SqlConnection(connectionString);
                if (dbConnection.State == ConnectionState.Closed || dbConnection.State == ConnectionState.Broken)
                {
                    dbConnection.Open();
                }
                return true;
            }
            catch
            {
                return false;
            }
        }

        /// <summary>
        /// Lấy danh sách database
        /// </summary>
        /// <returns></returns>
        public DataTable GetListDatabase()
        {
            var query = @"select database_id, name from sys.databases where name not in('master', 'tempdb', 'model', 'msdb')";
            return ExecuteDataTable(query);
        }

        /// <summary>
        /// Kiểm tra table có tồn tại
        /// </summary>
        /// <param name="databaseName"></param>
        /// <param name="tableName"></param>
        /// <returns></returns>
        public bool CheckTableExists(string databaseName, string tableName)
        {
            var query = string.Format(@"use {0}
                                        select 1 from INFORMATION_SCHEMA.TABLES where TABLE_NAME = '{1}'", databaseName, tableName);
            return ExecuteDataTable(query).Rows.Count > 0;
        }

        /// <summary>
        /// Lấy danh sách các cột của bảng
        /// </summary>
        /// <param name="databaseName"></param>
        /// <param name="tableName"></param>
        /// <returns></returns>
        public DataTable GetColumnTable(string databaseName, string tableName)
        {
            var query = string.Format(@"use {0}
                        select COLUMN_NAME, COLUMN_DEFAULT, IS_NULLABLE, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH 
                        from INFORMATION_SCHEMA.COLUMNS where TABLE_NAME = '{1}'", databaseName, tableName);
            return ExecuteDataTable(query);
        }

        /// <summary>
        /// Xóa database
        /// </summary>
        /// <param name="databaseName"></param>
        /// <returns></returns>
        public bool DropDatabase(string databaseName)
        {
            var query = string.Format(@"use master
                                        drop database {0}", databaseName);
            return ExecuteNonQuery(query) == -1;
        }

        /// <summary>
        /// Xóa bảng
        /// </summary>
        /// <param name="databaseName"></param>
        /// <param name="tableName"></param>
        /// <returns></returns>
        public bool DropTable(string databaseName, string tableName)
        {
            var query = string.Format(@"use {0}
                                        drop table {0}", databaseName, tableName);
            return ExecuteNonQuery(query) == -1;
        }

        /// <summary>
        /// Lấy cấu trúc của bảng
        /// </summary>
        /// <param name="databaseName"></param>
        /// <param name="tableName"></param>
        /// <returns></returns>
        public DataTable GetStructureTable(string databaseName, string tableName)
        {
            var query = string.Format(@"use {0}
                                        select top 0 * from {1}", databaseName, tableName);
            return ExecuteDataTable(query);
        }

        /// <summary>
        /// Kiểm tra store procedure có tồn tại
        /// </summary>
        /// <param name="databaseName"></param>
        /// <param name="storeProcedure"></param>
        /// <returns></returns>
        public bool CheckStoreProcedureExists(string databaseName, string storeProcedure)
        {
            var query = string.Format(@"use {0}
                                        select 1 from sys.objects WHERE type = 'P' and name = '{1}'", databaseName, storeProcedure);
            return ExecuteDataTable(query, CommandType.Text).Rows.Count > 0;
        }

        /// <summary>
        /// Kiểm tra function có tồn tại
        /// </summary>
        /// <param name="databaseName"></param>
        /// <param name="functionName"></param>
        /// <returns></returns>
        public bool CheckFunctionExists(string databaseName, string functionName)
        {
            var query = string.Format(@"use {0}
                                    if OBJECT_ID('{1}') is not null
                                    begin
	                                    select 1
                                    end", databaseName, functionName);
            return ExecuteDataTable(query).Rows.Count > 0;
        }
    }
}
