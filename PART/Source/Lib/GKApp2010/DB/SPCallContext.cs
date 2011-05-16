﻿//------------------------------------------------------------------------------
// Gentofte Kommune application framework 2011.
// Copyright (c) Gentofte Kommune 2011.
// This source is subject to the Mozilla Public License 1.1 terms. See http://www.opensource.org/licenses/mozilla1.1.
//------------------------------------------------------------------------------

using System;
using System.Configuration;
using System.Data;
using System.Data.Common;

using Microsoft.Practices.EnterpriseLibrary.Data.Sql;

namespace GKApp2010.DB
{
    // ================================================================================
    public class StoredProcedureCallContext
    {
        private const string retValParameterName = "ReturnValue";

        private bool retValParameterDefined = false;
        private int retVal = int.MinValue;

        private bool standardParametersDefined = false;
        private string auxParameter = "";
        private string contextParameter = "";

        private SqlDatabase db = null;
        private DbCommand cmd = null;

        #region constructors
        // -----------------------------------------------------------------------------
        public StoredProcedureCallContext(string connStrID, string spName)
        {
            CommonInit(connStrID, spName, "", "");
        }

        // -----------------------------------------------------------------------------
        public StoredProcedureCallContext(string connStrID, string spName, string context)
        {
            CommonInit(connStrID, spName, context, "");
        }

        // -----------------------------------------------------------------------------
        public StoredProcedureCallContext(string connStrID, string spName, string context, string aux)
        {
            CommonInit(connStrID, spName, context, aux);
        }
        #endregion

        #region public methods
        // -----------------------------------------------------------------------------
        public void AddInParameter(string paramName, DbType paramType, object value)
        {
            db.AddInParameter(cmd, paramName, paramType, value);
        }

        // -----------------------------------------------------------------------------
        public void AddOutParameter(string paramName, DbType paramType)
        {
            this.AddOutParameter(paramName, paramType, GetDbTypeSize(paramType));
        }

        // -----------------------------------------------------------------------------
        public void AddOutParameter(string paramName, DbType paramType, int size)
        {
            db.AddOutParameter(cmd, paramName, paramType, size);
        }

        // -----------------------------------------------------------------------------
        public void AddInOutParameter(string paramName, DbType paramType, object value)
        {
            db.AddParameter(cmd, paramName, paramType, ParameterDirection.InputOutput, string.Empty, DataRowVersion.Default, value);
        }

        // -----------------------------------------------------------------------------
        public void AddInOutParameter(string paramName, DbType paramType, int size, object value)
        {
            db.AddParameter(cmd, paramName, paramType, size, ParameterDirection.InputOutput, true, 0, 0, string.Empty, DataRowVersion.Default, value);
        }

        // -----------------------------------------------------------------------------
        public object GetParameterValue(string paramName)
        {
            object val = db.GetParameterValue(cmd, paramName);
            return val;
        }

        // -----------------------------------------------------------------------------
        public string GetParameterStringValue(string paramName)
        {
            string result = "";

            object val = GetParameterValue(paramName);
            if (val != null)
            {
                result = (string)val;
            }

            return result;
        }

        // -----------------------------------------------------------------------------
        public Guid GetParameterGuidValue(string paramName)
        {
            Guid result = Guid.Empty;

            object val = GetParameterValue(paramName);
            if (val != null)
            {
                result = (Guid)val;
            }

            return result;
        }

        // -----------------------------------------------------------------------------
        public int GetParameterIntValue(string paramName)
        {
            int result = int.MinValue;

            object val = GetParameterValue(paramName);
            if (val != null)
            {
                result = (int)val;
            }

            return result;
        }

        // -----------------------------------------------------------------------------
        public DateTime GetParameterDateTimeValue(string paramName)
        {
            DateTime result = DateTime.MinValue;

            object val = GetParameterValue(paramName);
            if (val != null)
            {
                result = (DateTime)val;
            }

            return result;
        }

        // -----------------------------------------------------------------------------
        public int ExecuteScalar()
        {
            PrepareSPCall();

            object o = db.ExecuteScalar(cmd);
            CleanupAfterSPCall();

            return 0;
        }

        // -----------------------------------------------------------------------------
        public int ExecuteScalarNoStdParams()
        {
            object o = db.ExecuteScalar(cmd);
            return 0;
        }

        // -----------------------------------------------------------------------------
        public DataSet ExecuteDataSet()
        {
            PrepareSPCall();

            DataSet ds = db.ExecuteDataSet(cmd);
            CleanupAfterSPCall();

            return ds;
        }

        // -----------------------------------------------------------------------------
        public DataSet ExecuteDataSetNoStdParams()
        {
            DataSet ds = db.ExecuteDataSet(cmd);
            return ds;
        }

        // -----------------------------------------------------------------------------
        public int ExecuteNonQuery()
        {
            PrepareSPCall();

            int rowcnt = db.ExecuteNonQuery(cmd);
            CleanupAfterSPCall();

            return rowcnt;
        }

        // -----------------------------------------------------------------------------
        public int ExecuteNonQueryNoStdParams()
        {
            int rowcnt = db.ExecuteNonQuery(cmd);
            return rowcnt;
        }

        // -----------------------------------------------------------------------------
        public int ExecuteNonQueryWithReturnValue()
        {
            PrepareSPCall();

            db.ExecuteNonQuery(cmd);
            CleanupAfterSPCall();

            return ReturnValue;
        }

        // -----------------------------------------------------------------------------
        public static void ClearStandardParams(ref string context, ref string aux)
        {
            context = string.Empty;
            aux = string.Empty;
        }
        #endregion

        #region public properties
        // -----------------------------------------------------------------------------
        public string Context
        {
            set { contextParameter = value; }
            get { return contextParameter; }
        }

        // -----------------------------------------------------------------------------
        public string Aux
        {
            set { auxParameter = value; }
            get { return auxParameter; }
        }

        // -----------------------------------------------------------------------------
        public int ReturnValue
        {
            get { return retVal; }
        }
        #endregion

        #region private methods
        // -----------------------------------------------------------------------------
        private void CommonInit(string connStrID, string spName, string context, string aux)
        {
            try
            {
                connStrID = ConfigurationManager.ConnectionStrings[connStrID].ConnectionString;
                //connStr = WebConfigurationManager.ConnectionStrings[connStr].ConnectionString;

                db = new SqlDatabase(connStrID);
                cmd = db.GetStoredProcCommand(spName);

                Context = context;
                Aux = aux;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        // -----------------------------------------------------------------------------
        private int GetDbTypeSize(DbType paramType)
        {
            switch (paramType)
            {
                case DbType.Guid:
                    return 16;

                case DbType.Int32:
                    return 4;

                case DbType.Int64:
                    return 8;

                // TODO Check iff this should bes eet to zero, when it's a time param. Check also the other fixed length param for same issue
                case DbType.DateTime:
                    return 8;
            }

            return -1;
        }

        // -----------------------------------------------------------------------------
        private void PrepareSPCall()
        {
            AddStandardCallParameters();
            AddReturnValueParameter();
        }

        // -----------------------------------------------------------------------------
        private void AddStandardCallParameters()
        {
            if (!standardParametersDefined)
            {
                AddInParameter("context", DbType.String, Context);
                AddInOutParameter("aux", DbType.String, 1020, Aux);

                standardParametersDefined = true;
            }
        }

        // -----------------------------------------------------------------------------
        private void AddReturnValueParameter()
        {
            retVal = int.MinValue;

            if (!retValParameterDefined)
            {
                db.AddParameter(cmd, retValParameterName, DbType.Int32, ParameterDirection.ReturnValue, string.Empty, DataRowVersion.Default, null);
                retValParameterDefined = true;
            }
        }

        // -----------------------------------------------------------------------------
        private void CleanupAfterSPCall()
        {
            Aux = GetParameterStringValue("aux");

            object val = db.GetParameterValue(cmd, retValParameterName);
            if (val != null)
            {
                retVal = (int)val;
            }
        }
        #endregion
    }
}
