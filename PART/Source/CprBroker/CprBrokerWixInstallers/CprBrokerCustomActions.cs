﻿/* ***** BEGIN LICENSE BLOCK *****
 * Version: MPL 1.1/GPL 2.0/LGPL 2.1
 *
 * The contents of this file are subject to the Mozilla Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://www.mozilla.org/MPL/
 *
 * Software distributed under the License is distributed on an "AS IS"basis,
 * WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
 * for the specific language governing rights and limitations under the
 * License.
 *
 * The CPR Broker concept was initally developed by
 * Gentofte Kommune / Municipality of Gentofte, Denmark.
 * Contributor(s):
 * Steen Deth
 *
 *
 * The Initial Code for CPR Broker and related components is made in
 * cooperation between Magenta, Gentofte Kommune and IT- og Telestyrelsen /
 * Danish National IT and Telecom Agency
 *
 * Contributor(s):
 * Beemen Beshara
 * Niels Elgaard Larsen
 * Leif Lodahl
 * Steen Deth
 *
 * The code is currently governed by IT- og Telestyrelsen / Danish National
 * IT and Telecom Agency
 *
 * Alternatively, the contents of this file may be used under the terms of
 * either the GNU General Public License Version 2 or later (the "GPL"), or
 * the GNU Lesser General Public License Version 2.1 or later (the "LGPL"),
 * in which case the provisions of the GPL or the LGPL are applicable instead
 * of those above. If you wish to allow use of your version of this file only
 * under the terms of either the GPL or the LGPL, and not to allow others to
 * use your version of this file under the terms of the MPL, indicate your
 * decision by deleting the provisions above and replace them with the notice
 * and other provisions required by the GPL or the LGPL. If you do not delete
 * the provisions above, a recipient may use your version of this file under
 * the terms of any one of the MPL, the GPL or the LGPL.
 *
 * ***** END LICENSE BLOCK ***** */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.Deployment.WindowsInstaller;
using CprBroker.Installers;
using CprBroker.Data.Part;
using CprBroker.Data.Applications;
using CprBroker.Utilities;
using CprBroker.Engine;
using CprBroker.EventBroker.Data;
using CprBroker.Installers.EventBrokerInstallers;
using System.Data.SqlClient;
using CprBroker.Utilities.Config;

namespace CprBrokerWixInstallers
{
    public partial class CprBrokerCustomActions
    {
        [CustomAction]
        public static ActionResult CalculateExecutionElevated(Session session)
        {
            return ProductCustomActions.CalculateExecutionElevated(session);
        }

        [CustomAction]
        public static ActionResult ReEvaluateLaunchConditions(Session session)
        {
            return ProductCustomActions.ReEvaluateLaunchConditions(session);
        }

        [CustomAction]
        public static ActionResult SetNetworkServiceUserName(Session session)
        {
            return ProductCustomActions.SetNetworkServiceUserName(session);
        }

        [CustomAction]
        public static ActionResult AfterInstallInitialize_Product(Session session)
        {
            try
            {
                return ProductCustomActions.AfterInstallInitialize_Product(session);
            }
            catch (Exception ex)
            {
                session.ShowErrorMessage(ex);
                throw ex;
            }
        }

        [CustomAction]
        public static ActionResult ForgetOlderVersions(Session session)
        {
            try
            {
                return ProductCustomActions.ForgetOlderVersions(session);
            }
            catch (Exception ex)
            {
                session.ShowErrorMessage(ex);
                throw ex;
            }
        }

        [CustomAction]
        public static ActionResult AppSearch_DB(Session session)
        {
            try
            {
                return DatabaseCustomAction.AppSearch_DB(session, true);
            }
            catch (Exception ex)
            {
                session.ShowErrorMessage(ex);
                throw ex;
            }
        }

        [CustomAction]
        public static ActionResult PreDatabaseDialog(Session session)
        {
            try
            {
                return DatabaseCustomAction.PreDatabaseDialog(session);
            }
            catch (Exception ex)
            {
                session.ShowErrorMessage(ex);
                throw ex;
            }
        }

        [CustomAction]
        public static ActionResult AfterDatabaseDialog(Session session)
        {
            try
            {
                return DatabaseCustomAction.AfterDatabaseDialog(session, true);
            }
            catch (Exception ex)
            {
                session.ShowErrorMessage(ex);
                throw ex;
            }
        }

        [CustomAction]
        public static ActionResult AfterInstallInitialize_DB(Session session)
        {
            try
            {
                return DatabaseCustomAction.AfterInstallInitialize_DB(session);
            }
            catch (Exception ex)
            {
                session.ShowErrorMessage(ex);
                throw ex;
            }
        }

        [CustomAction]
        public static ActionResult DeployDatabase(Session session)
        {
            try
            {
                // Prepare SQL scripts
                var createDatabaseObjectsSql = new Dictionary<string, string>();

                createDatabaseObjectsSql["CPR"] =
                    Properties.ResourcesExtensions.AllCprBrokerDatabaseObjectsSql;

                createDatabaseObjectsSql["EVENT"] = CprBroker.Installers.EventBrokerInstallers.Properties.ResourcesExtensions.AllEventBrokerDatabaseObjectsSql;

                // Prepare lookups
                var lookupDataArray = new Dictionary<string, KeyValuePair<string, string>[]>();

                lookupDataArray["CPR"] = Properties.ResourcesExtensions.Lookups;
                lookupDataArray["EVENT"] = CprBroker.Installers.EventBrokerInstallers.Properties.ResourcesExtensions.Lookups;

                // Custom methods
                var customMethods = new Dictionary<string, Action<SqlConnection>>();
                customMethods["CPR"] =
                    conn => CprBroker.Providers.CPRDirect.Authority.ImportText(Properties.Resources.Authority_4357, conn);

                return DatabaseCustomAction.DeployDatabase(session, createDatabaseObjectsSql, lookupDataArray, customMethods);
            }
            catch (Exception ex)
            {
                session.ShowErrorMessage(ex);
                throw ex;
            }
        }

        [CustomAction]
        public static ActionResult RemoveDatabase(Session session)
        {
            try
            {
                return DatabaseCustomAction.RemoveDatabase(session, true);
            }
            catch (Exception ex)
            {
                session.ShowErrorMessage(ex);
                throw ex;
            }
        }

        [CustomAction]
        public static ActionResult RollbackDatabase(Session session)
        {
            try
            {
                return DatabaseCustomAction.RemoveDatabase(session, false);
            }
            catch (Exception ex)
            {
                session.ShowErrorMessage(ex);
                throw ex;
            }
        }

        [CustomAction]
        public static ActionResult PatchDatabase(Session session)
        {
            try
            {
                var lineSep = Environment.NewLine + "GO" + Environment.NewLine;
                var patchInfos = new Dictionary<string, DatabasePatchInfo[]>();
                patchInfos["CPR"] = new DatabasePatchInfo[]{
                    new DatabasePatchInfo(){ 
                        Version = new Version(1,3), 
                        SqlScript = Properties.Resources.PatchDatabase_1_3, 
                        PatchAction = conn => CprBroker.Providers.CPRDirect.Authority.ImportText(Properties.Resources.Authority_4357, conn)
                    },
                    new DatabasePatchInfo(){ 
                        Version = new Version(1,3,2), 
                        SqlScript = Properties.Resources.PatchDatabase_1_3_2, 
                        PatchAction = null
                    },
                    new DatabasePatchInfo(){ 
                        Version = new Version(1,4), 
                        SqlScript = Properties.Resources.PatchDatabase_1_4, 
                        PatchAction = null
                    },
                    new DatabasePatchInfo(){ 
                        Version = new Version(2,1),
                        SqlScript = Properties.Resources.PatchDatabase_2_1,
                        PatchAction = null
                    },
                    new DatabasePatchInfo(){ 
                        Version = new Version(2,2),
                        SqlScript = Properties.Resources.PatchDatabase_2_2,
                        PatchAction = null
                    },
                    new DatabasePatchInfo(){ 
                        Version = new Version(2,2,1),
                        SqlScript = Properties.Resources.PatchDatabase_2_2_1,
                        PatchAction = null
                    },
                    new DatabasePatchInfo(){ 
                        Version = new Version(2,2,2),
                        SqlScript = Properties.Resources.PatchDatabase_2_2_2,
                        PatchAction = conn=> DatabaseCustomAction.InsertLookup<CprBroker.Data.DataProviders.BudgetInterval>(Properties.Resources.BudgetInterval_Csv, conn) 
                    },
                    new DatabasePatchInfo(){ 
                        Version = new Version(2,2,3),
                        SqlScript = Properties.Resources.PatchDatabase_2_2_3,
                        PatchAction = conn=> 
                            {
                                DatabaseCustomAction.InsertLookup<CprBroker.Data.Queues.DbQueue>(Properties.Resources.Queue_Csv, conn);
                                CprBroker.Providers.CPRDirect.Authority.ImportText(Properties.Resources.Authority_4357, conn);
                            }
                    },
                    new DatabasePatchInfo(){ 
                        Version = new Version(2,2,3),
                        SqlScript = string.Join(
                            lineSep, 
                            new string[]{
                                Properties.Resources.PatchDatabase_2_2_3,
                                Properties.Resources.PersonSearchCache,
                                Properties.Resources.TrimAddressString,
                                Properties.Resources.InitializePersonSearchCache}),
                        PatchAction = null
                    },
                };

                patchInfos["EVENT"] = new DatabasePatchInfo[] { 
                    new DatabasePatchInfo(){
                        Version = new Version(2,2),
                        SqlScript = CprBroker.Installers.EventBrokerInstallers.Properties.Resources.PatchDatabase_2_2,
                        PatchAction = null
                    },
                    new DatabasePatchInfo(){
                        Version = new Version(2,2,3),
                        SqlScript = string.Join(
                            lineSep,
                            new string[]{
                                CprBroker.Installers.EventBrokerInstallers.Properties.Resources.PatchDatabase_2_2_3 ,
                                CprBroker.Installers.EventBrokerInstallers.Properties.ResourcesExtensions.AllEventBrokerStoredProceduresSql}),
                        PatchAction = null
                    }
                };

                var result = DatabaseCustomAction.PatchDatabase(session, patchInfos);
                return ActionResult.Success;
            }
            catch (Exception ex)
            {
                session.ShowErrorMessage(ex);
                throw ex;
            }
        }

        [CustomAction]
        public static ActionResult PopulateWebsites(Session session)
        {
            try
            {
                return WebsiteCustomAction.PopulateWebsites(session);
            }
            catch (Exception ex)
            {
                session.ShowErrorMessage(ex);
                throw ex;
            }
        }


        [CustomAction]
        public static ActionResult AppSearch_WEB(Session session)
        {
            try
            {
                return WebsiteCustomAction.AppSearch_WEB(session);
            }
            catch (Exception ex)
            {
                session.ShowErrorMessage(ex);
                throw ex;
            }
        }

        [CustomAction]
        public static ActionResult PreWebDialog(Session session)
        {
            try
            {
                return WebsiteCustomAction.PreWebDialog(session);
            }
            catch (Exception ex)
            {
                session.ShowErrorMessage(ex);
                throw ex;
            }
        }

        [CustomAction]
        public static ActionResult AfterWebDialog(Session session)
        {
            try
            {
                return WebsiteCustomAction.AfterWebDialog(session);
            }
            catch (Exception ex)
            {
                session.ShowErrorMessage(ex);
                throw ex;
            }
        }

        [CustomAction]
        public static ActionResult AfterInstallInitialize_WEB(Session session)
        {
            try
            {
                string[] extraCustomActionNames = new string[]
                {
                    "InstallBackendService",
                    "RollbackBackendService",
                    "UnInstallBackendService",
                    "SetCprBrokerUrl",
                    "CloneDataProviderSectionsToBackendService",
                    "InitTasksConfiguration"
                };
                return WebsiteCustomAction.AfterInstallInitialize_WEB(session, extraCustomActionNames);
            }
            catch (Exception ex)
            {
                session.ShowErrorMessage(ex);
                throw ex;
            }
        }

        [CustomAction]
        public static ActionResult CreateWebsite(Session session)
        {
            try
            {
                // Reset types in config filr
                ResetDataProviderSectionDefinitions(session);

                // Now prepare and install the website
                var allOptions = new Dictionary<string, WebInstallationOptions>();

                Dictionary<string, string> connectionStrings = new Dictionary<string, string>();
                connectionStrings["CprBroker.Config.Properties.Settings.CprBrokerConnectionString"] = DatabaseSetupInfo.CreateFromFeature(session, "CPR").CreateConnectionString(false, true);

                WebInstallationOptions cprOptions = new WebInstallationOptions()
                {
                    EncryptConnectionStrings = true,
                    ConnectionStrings = new Dictionary<string, string>(connectionStrings),
                    InitializeFlatFileLogging = true,
                    WebsiteDirectoryRelativePath = EventBrokerCustomActions.PathConstants.CprBrokerWebsiteDirectoryRelativePath,
                    ConfigSectionGroupEncryptionOptions = new ConfigSectionGroupEncryptionOptions[]
                {
                    new ConfigSectionGroupEncryptionOptions()
                    {
                        ConfigSectionGroupName = Constants.DataProvidersSectionGroupName,
                        ConfigSectionEncryptionOptions = new ConfigSectionEncryptionOptions[]
                        {
                            new ConfigSectionEncryptionOptions(){ 
                                SectionName = DataProviderKeysSection.SectionName, 
                                SectionType=typeof(DataProviderKeysSection), 
                                CustomMethod = config=>DataProviderKeysSection.RegisterNewKeys(config)
                            },
                            new ConfigSectionEncryptionOptions(){ 
                                SectionName = DataProvidersConfigurationSection.SectionName, 
                                SectionType=typeof(DataProvidersConfigurationSection),
                                CustomMethod =null
                            }
                        }
                    }
                }
                };
                allOptions["CPR"] = cprOptions;

                connectionStrings["CprBroker.Config.Properties.Settings.EventBrokerConnectionString"] = DatabaseSetupInfo.CreateFromFeature(session, "EVENT").CreateConnectionString(false, true);
                WebInstallationOptions eventOptions = new WebInstallationOptions()
                {
                    EncryptConnectionStrings = false,
                    ConnectionStrings = connectionStrings,
                    InitializeFlatFileLogging = true,
                    WebsiteDirectoryRelativePath = EventBrokerCustomActions.PathConstants.EventBrokerWebsiteDirectoryRelativePath,
                    ConfigSectionGroupEncryptionOptions = new ConfigSectionGroupEncryptionOptions[]
                    {
                        new ConfigSectionGroupEncryptionOptions()
                        {
                            ConfigSectionGroupName = Constants.DataProvidersSectionGroupName,
                            ConfigSectionEncryptionOptions = new ConfigSectionEncryptionOptions[]
                            {
                                new ConfigSectionEncryptionOptions(){ 
                                    SectionName = DataProviderKeysSection.SectionName, 
                                    SectionType=typeof(DataProviderKeysSection), 
                                    // DO NOT create new encryption keys here
                                    //CustomMethod = config=>DataProviderKeysSection.RegisterNewKeys(config)
                                },
                                new ConfigSectionEncryptionOptions(){ 
                                    SectionName = DataProvidersConfigurationSection.SectionName, 
                                    SectionType=typeof(DataProvidersConfigurationSection),
                                    CustomMethod =null
                                }
                            }
                        }
                    }
                };
                allOptions["EVENT"] = eventOptions;

                return WebsiteCustomAction.DeployWebsite(session, allOptions);
            }
            catch (Exception ex)
            {
                session.ShowErrorMessage(ex);
                throw ex;
            }
        }

        [CustomAction]
        public static ActionResult RollbackWebsite(Session session)
        {
            try
            {
                return WebsiteCustomAction.RollbackWebsite(session);
            }
            catch (Exception ex)
            {
                session.ShowErrorMessage(ex);
                throw ex;
            }
        }

        [CustomAction]
        public static ActionResult RemoveWebsite(Session session)
        {
            try
            {
                return WebsiteCustomAction.RemoveWebsite(session);
            }
            catch (Exception ex)
            {
                session.ShowErrorMessage(ex);
                throw ex;
            }
        }

        [CustomAction]
        public static ActionResult PatchWebsite(Session session)
        {
            try
            {
                var featurePatchInfos = new Dictionary<string, WebPatchInfo[]>();

                featurePatchInfos["CPR"] = new WebPatchInfo[] { 
                    new WebPatchInfo()
                    { 
                        Version = new Version(1,3),
                        PatchAction = () => PatchWebsite_1_3_0(session)
                    },
                    new WebPatchInfo(){
                        Version = new Version(2,1,1),
                        PatchAction = () => PatchWebsite_2_1_1(session)
                    },
                    new WebPatchInfo(){
                        Version = new Version(2,2,2),
                        PatchAction = () => PatchWebsite_2_2_2(session)
                    },
                    new WebPatchInfo(){
                        Version = new Version(2,2,3),
                        PatchAction = () => PatchWebsite_2_2_3(session)
                    }
                };

                featurePatchInfos["EVENT"] = new WebPatchInfo[] { 
                    new WebPatchInfo()
                    { 
                        Version = new Version(2,2),
                        PatchAction = () => EventBrokerCustomActions.MoveBackendServiceToNewLocation(session)
                    }
                };

                return WebsiteCustomAction.PatchWebsite(session, featurePatchInfos);
            }
            catch (Exception ex)
            {
                session.ShowErrorMessage(ex);
                throw ex;
            }
        }

    }
}
