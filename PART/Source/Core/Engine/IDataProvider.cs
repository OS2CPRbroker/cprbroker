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
using CprBroker.Schemas;
using CprBroker.Schemas.Part;
using CprBroker.Data;

namespace CprBroker.Engine
{
    public interface IDataProvider
    {
        /// <summary>
        /// Ensure that the current provider is alive
        /// </summary>
        /// <returns></returns>
        bool IsAlive();

        /// <summary>
        /// GetPropertyValuesOfType the current version for this provider
        /// </summary>
        //TODO: Remove this property fromDate all data providers
        Version Version { get; }
    }

    /// <summary>
    /// Represents an external data provider (DPR, KMD)
    /// </summary> 
    public interface IExternalDataProvider : IDataProvider, IHasConfigurationProperties
    {

    }

    public enum LocalProxyUsageOptions
    {
        Default,
        BeforeLocal,
        AfterLocal
    }

    public interface ILocalProxyDataProvider
    {
        LocalProxyUsageOptions LocalProxyUsage { get; set; }
    }

    public interface IAutoUpdateDataProvider
    {
        bool AutoUpdate { get; }
        bool IsReady { get; }
        string AutoUpdateHint { get; }
        bool InitAutoUpdate();
    }

    public interface IChangePuller<T>
    {
        IEnumerable<T> GetChanges(int batchSize, TimeSpan delay);
        void DeleteChanges(IEnumerable<T> changes);
    }

    /// <summary>
    /// A data provider that supports update detection
    /// </summary>
    /// <typeparam name="TKey"></typeparam>
    public interface IUpdatableDataProvider<TKey>
    {
        TKey[] ReadUpdateQueue(int batchSize);
        void DeleteFromQueue(TKey[] keys);
    }

    /// <summary>
    /// Contains methods related to application management
    /// </summary>
    public interface IApplicationManager : IDataProvider
    {
        /// <summary>
        /// Creates a new application with the given name
        /// </summary>
        /// <param name="userToken">Security Token for authorizing the current user</param>        
        /// <param name="name">Application name</param>
        /// <returns></returns>
        ApplicationType RequestAppRegistration(string name);

        /// <summary>
        /// Approves an application that has the given token
        /// </summary>
        /// <param name="userToken">Security Token for authorizing the current user</param>
        /// <param name="appToken">Security Token for the application the current application.</param>
        /// <param name="targetAppToken">Application token of the application to be approved</param>
        /// <returns></returns>
        bool ApproveAppRegistration(string targetAppToken);

        /// <summary>
        /// Returns a list of all applications
        /// </summary>
        /// <param name="userToken">Security Token for authorizing the current user</param>
        /// <param name="appToken">Security Token for the application the current application.</param>
        /// <returns></returns>
        ApplicationType[] ListAppRegistration();

        /// <summary>
        /// Unregisters the application with the supplied token
        /// </summary>
        /// <param name="userToken">Security Token for authorizing the current user</param>
        /// <param name="appToken">Security Token for the application the current application.</param>
        /// <param name="targetAppToken">Token of the application that will be removed</param>
        /// <returns></returns>
        bool UnregisterApp(string targetAppToken);
    }
}