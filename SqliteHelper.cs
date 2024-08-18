using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SQLite;
using System.Text;

namespace cobeNET
{
    public static class SqliteHelper
    {
        public static int ExecuteWrite(this SQLiteConnection con, string query, Dictionary<string, object> args = null)
        {
            int numberOfRowsAffected;

            //setup the connection to the database
            using (var connection = new SQLiteConnection(con.ConnectionString)) // TODO - dont create new object..
            {
                connection.Open();

                //open a new command
                using (var cmd = new SQLiteCommand(query, connection))
                {
                    //set the arguments given in the query
                    if (args != null)
                    {
                        foreach (var pair in args)
                        {
                            cmd.Parameters.AddWithValue(pair.Key, pair.Value);
                        }
                    }

                    //execute the query and get the number of row affected
                    numberOfRowsAffected = cmd.ExecuteNonQuery();
                }

                return numberOfRowsAffected;
            }
        }

        public static long ExecuteWriteEffected(this SQLiteConnection con, string query, Dictionary<string, object> args = null)
        {
            long lastRowIdAffected;

            //setup the connection to the database
            using (var connection = new SQLiteConnection(con.ConnectionString)) // TODO - dont create new object..
            {
                connection.Open();

                //open a new command
                using (var cmd = new SQLiteCommand(query, connection))
                {
                    //set the arguments given in the query
                    if (args != null)
                    {
                        foreach (var pair in args)
                        {
                            cmd.Parameters.AddWithValue(pair.Key, pair.Value);
                        }
                    }

                    //execute the query and get the number of row affected
                    cmd.ExecuteNonQuery();

                    lastRowIdAffected = connection.LastInsertRowId;
                }

                return lastRowIdAffected;
            }
        }

        public static DataTable Execute(this SQLiteConnection con, string query, Dictionary<string, object> args = null)
        {
            if (string.IsNullOrEmpty(query.Trim()))
                return null;

            using (var connection = new SQLiteConnection(con.ConnectionString))
            {
                connection.Open();
                using (var cmd = new SQLiteCommand(query, connection))
                {
                    if (args != null)
                    {
                        foreach (KeyValuePair<string, object> entry in args)
                        {
                            cmd.Parameters.AddWithValue(entry.Key, entry.Value);
                        }
                    }

                    var da = new SQLiteDataAdapter(cmd);

                    var dt = new DataTable();
                    da.Fill(dt);

                    da.Dispose();
                    return dt;
                }
            }
        }

    }
}
