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
using System.Diagnostics;
using System.Threading;
using System.Data.Linq;
using CprBroker.Schemas.Part;
using CprBroker.Utilities;

namespace CprBroker.Engine
{
    /// <summary>
    /// The main class of the system's engine.
    /// Manages calls to data providers
    /// </summary>
    public static partial class Manager
    {

        public static BasicOutputType<TItem> GetMethodOutput<TItem>(GenericFacadeMethodInfo<TItem> facade)
        {
            return GetMethodOutput<BasicOutputType<TItem>, TItem>(facade);
        }

        public static TOutput GetMethodOutput<TOutput, TItem>(FacadeMethodInfo<TOutput, TItem> facade) where TOutput : class, IBasicOutput<TItem>, new()
        {
            try
            {
                #region Initialization and loading of data providers
                // Initialize context
                try
                {
                    BrokerContext.Initialize(facade.ApplicationToken, facade.UserToken);
                }
                catch (Exception ex)
                {
                    return new TOutput()
                    {
                        StandardRetur = StandardReturType.InvalidApplicationToken(facade.ApplicationToken)
                    };
                }

                // Validate
                StandardReturType validationRet = facade.ValidateInput();
                if (!StandardReturType.IsSucceeded(validationRet))
                {
                    Local.Admin.AddNewLog(TraceEventType.Error, BrokerContext.Current.WebMethodMessageName, TextMessages.InvalidInput, null, null);
                    if (validationRet == null)
                    {
                        validationRet = StandardReturType.UnspecifiedError("Validation failed");
                    }
                    return new TOutput()
                    {
                        StandardRetur = validationRet
                    };
                }

                // Initialize facade method
                facade.Initialize();

                // have a list of clearData provider types and corresponding methods to call
                var subMethodRunStates = facade.SubMethodInfos
                    .Select(mi => new SubMethodRunState()
                    {
                        SubMethodInfo = mi,
                        DataProviders = DataProviderManager.GetDataProviderList(mi.InterfaceType, mi.LocalDataProviderOption)
                    })
                    .ToArray();

                // Now check that each method call info either has at least one clearData provider implementation or can be safely ignored. 
                var missingDataProvidersExist = subMethodRunStates.Where(mi => mi.SubMethodInfo.FailIfNoDataProvider && mi.DataProviders.Count == 0).FirstOrDefault() != null;

                if (missingDataProvidersExist)
                {
                    Local.Admin.AddNewLog(TraceEventType.Warning, BrokerContext.Current.WebMethodMessageName, TextMessages.NoDataProvidersFound, null, null);
                    return new TOutput()
                    {
                        StandardRetur = StandardReturType.Create(HttpErrorCode.DATASOURCE_UNAVAILABLE)
                    };
                }
                #endregion

                #region creation of sub results in threads
                // Catch the current broker context in a local variable
                var currentBrokerContext = BrokerContext.Current;
                long finishedThreads = 0;
                // Now create the sub results
                for (int iSubMethod = 0; iSubMethod < subMethodRunStates.Length; iSubMethod++)
                {

                    subMethodRunStates[iSubMethod].ThreadStart = new ParameterizedThreadStart((o) =>
                        {
                            // Copy the broker context to this new thread
                            BrokerContext.Current = currentBrokerContext;

                            SubMethodRunState subMethodInfo = subMethodRunStates[(int)o];
                            // Loop over clearData providers until one succeeds
                            foreach (IDataProvider prov in subMethodInfo.DataProviders)
                            {
                                try
                                {
                                    object subResult = subMethodInfo.SubMethodInfo.Invoke(prov);
                                    // See if result can be used to update local database
                                    if (prov is IExternalDataProvider && subMethodInfo.SubMethodInfo.IsUpdatableOutput(subResult))
                                    {
                                        try
                                        {
                                            subMethodInfo.SubMethodInfo.InvokeUpdateMethod(subResult);
                                        }
                                        catch (Exception updateException)
                                        {
                                            string xml = Strings.SerializeObject(subResult);
                                            Local.Admin.LogException(updateException);
                                        }
                                    }
                                    // Exit loop if succeeded
                                    if (subMethodInfo.SubMethodInfo.IsSuccessfulOutput(subResult))
                                    {
                                        subMethodInfo.Result = subResult;
                                        subMethodInfo.Succeeded = true;
                                        break;
                                    }
                                }
                                catch (Exception dataProviderException)
                                {
                                    Local.Admin.LogException(dataProviderException);
                                }
                            }

                            if (!subMethodInfo.Succeeded)
                            {
                                Local.Admin.AddNewLog(TraceEventType.Information, BrokerContext.Current.WebMethodMessageName, TextMessages.AllDataProvidersFailed + subMethodInfo.SubMethodInfo, null, null);
                            }

                            // Signal the end of processing
                            System.Threading.Interlocked.Increment(ref finishedThreads);
                        }
                    );

                    if (subMethodRunStates.Length > 1)
                    {
                        subMethodRunStates[iSubMethod].Thread = new Thread(subMethodRunStates[iSubMethod].ThreadStart);
                        subMethodRunStates[iSubMethod].Thread.Start(iSubMethod);
                    }
                }
                #endregion

                #region Threads control
                if (subMethodRunStates.Length > 1)
                {
                    // Wait for sub results to continue
                    DateTime executionStartTime = DateTime.Now;

                    while (System.Threading.Interlocked.Read(ref finishedThreads) < subMethodRunStates.Length && (DateTime.Now - executionStartTime).TotalMilliseconds < Config.Properties.Settings.Default.DataProviderMillisecondsTimeout)
                    {
                        int waitMilliseconds = 100;
                        Thread.Sleep(waitMilliseconds);
                    }
                    foreach (var mi in subMethodRunStates)
                    {
                        mi.Thread.Abort();
                    }
                }
                else
                {
                    subMethodRunStates[0].ThreadStart.Invoke(0);
                }
                #endregion

                #region Final aggregation

                var succeededCount = (from mi in subMethodRunStates where mi.Succeeded select mi).Count();
                var subResults = (from mi in subMethodRunStates select mi.Result).ToArray();

                bool canAggregate = facade.AggregationFailOption == AggregationFailOption.FailNever
                    || facade.AggregationFailOption == AggregationFailOption.FailOnAll && succeededCount > 0
                    || facade.AggregationFailOption == AggregationFailOption.FailOnAny && succeededCount == subMethodRunStates.Length;

                if (canAggregate)
                {
                    var outputMainItem = facade.Aggregate(subResults);
                    if (facade.IsValidResult(outputMainItem))
                    {
                        Local.Admin.AddNewLog(TraceEventType.Information, BrokerContext.Current.WebMethodMessageName, TextMessages.Succeeded, null, null);
                        var output = new TOutput();
                        output.SetMainItem(outputMainItem);
                        if (succeededCount == subMethodRunStates.Length)
                        {
                            output.StandardRetur = StandardReturType.OK();
                        }
                        else
                        {
                            var failedInput = subMethodRunStates.Where(smi => !smi.Succeeded).Select(smi => smi.SubMethodInfo.InputToString()).ToArray();
                            output.StandardRetur = StandardReturType.PartialSuccess(failedInput);
                        }

                        return output;
                    }
                    else
                    {
                        string xml = Strings.SerializeObject(outputMainItem);
                        Local.Admin.AddNewLog(TraceEventType.Error, BrokerContext.Current.WebMethodMessageName, TextMessages.ResultGatheringFailed, typeof(TOutput).ToString(), xml);
                        return new TOutput() { StandardRetur = StandardReturType.UnspecifiedError("Aggregation failed") };
                    }
                }
                else
                {
                    return new TOutput() { StandardRetur = StandardReturType.Create(HttpErrorCode.DATASOURCE_UNAVAILABLE) };
                }
                #endregion

            }
            catch (Exception ex)
            {
                Local.Admin.LogException(ex);
                return new TOutput() { StandardRetur = StandardReturType.UnspecifiedError() };
            }
        }

        private class SubMethodRunState
        {
            public SubMethodInfo SubMethodInfo;
            public List<IDataProvider> DataProviders;
            public object Result = null;
            public ParameterizedThreadStart ThreadStart;
            public Thread Thread;
            public bool Succeeded = false;
        }
    }
}
