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
using System.Diagnostics;
using System.Threading;
using System.Data.Linq;
using CprBroker.Schemas.Part;
using CprBroker.Utilities;
using CprBroker.Utilities.Config;

namespace CprBroker.Engine
{
    /// <summary>
    /// The main class of the system's engine.
    /// Manages calls to data providers
    /// </summary>
    public class RequestProcessor
    {
        public RequestProcessor()
        { }

        public BasicOutputType<TOutputMainItem> GetMethodOutput<TOutputMainItem>(GenericFacadeMethodInfo<TOutputMainItem> facade)
        {
            return GetMethodOutput<SubMethodInfo, BasicOutputType<TOutputMainItem>, TOutputMainItem>(facade);
        }

        public TOutput GetMethodOutput<TSubMethod, TOutput, TOutputMainItem>(FacadeMethodInfo<TSubMethod, TOutput, TOutputMainItem> facade)
            where TOutput : class, IBasicOutput<TOutputMainItem>, new()
            where TSubMethod : SubMethodInfo
        {
            try
            {
                StandardReturType standardRetur;
                SubMethodRunState<TSubMethod>[] subMethodRunStates;

                standardRetur = Validate<TSubMethod, TOutput, TOutputMainItem>(facade);
                if (!StandardReturType.IsSucceeded(standardRetur))
                {
                    return new TOutput() { StandardRetur = standardRetur };
                }


                standardRetur = Initialize<TSubMethod, TOutput, TOutputMainItem>(facade, out subMethodRunStates);
                if (!StandardReturType.IsSucceeded(standardRetur))
                {
                    return new TOutput() { StandardRetur = standardRetur };
                }

                RunThreads<TSubMethod, TOutput, TOutputMainItem>(facade, subMethodRunStates);
                return AggregateResults<TSubMethod, TOutput, TOutputMainItem>(facade, subMethodRunStates);
            }
            catch (Exception ex)
            {
                Local.Admin.LogException(ex);
                return new TOutput() { StandardRetur = StandardReturType.UnspecifiedError() };
            }
        }

        public StandardReturType Validate<TSubMethod, TOutput, TOutputMainItem>(FacadeMethodInfo<TSubMethod, TOutput, TOutputMainItem> facade)
            where TOutput : class, IBasicOutput<TOutputMainItem>, new()
            where TSubMethod : SubMethodInfo
        {
            // Initialize context
            try
            {
                BrokerContext.Initialize(facade.ApplicationToken, facade.UserToken);
            }
            catch (Exception ex)
            {
                return StandardReturType.InvalidApplicationToken(facade.ApplicationToken);
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
                return validationRet;
            }
            return StandardReturType.OK();
        }

        public StandardReturType Initialize<TSubMethod, TOutput, TMainOutputItem>(
            FacadeMethodInfo<TSubMethod, TOutput, TMainOutputItem> facade,
            out SubMethodRunState<TSubMethod>[] subMethodRunStates
        )
            where TOutput : class, IBasicOutput<TMainOutputItem>, new()
            where TSubMethod : SubMethodInfo
        {
            // Initialize facade method
            facade.Initialize();

            // have a list of data provider types and corresponding methods to call
            bool missingDataProvidersExist;
            subMethodRunStates = facade.CreateSubMethodRunStates(out missingDataProvidersExist);

            facade.RegisterSubMethodOperations(BrokerContext.Current);

            if (missingDataProvidersExist)
            {
                Local.Admin.AddNewLog(TraceEventType.Warning, BrokerContext.Current.WebMethodMessageName, TextMessages.NoDataProvidersFound, null, null);
                return StandardReturType.Create(HttpErrorCode.DATASOURCE_UNAVAILABLE);
            }
            return StandardReturType.OK();
        }

        public void RunThreads<TSubMethod, TOutput, TOutputMainItem>(FacadeMethodInfo<TSubMethod, TOutput, TOutputMainItem> facade, SubMethodRunState<TSubMethod>[] subMethodRunStates)
            where TOutput : class, IBasicOutput<TOutputMainItem>, new()
            where TSubMethod : SubMethodInfo
        {
            #region Creation of sub results in threads
            // Catch the current broker context in a local variable
            var currentBrokerContext = BrokerContext.Current;
            var currentHttpContext = System.Web.HttpContext.Current;
            long finishedThreads = 0;
            // Now create the sub results
            for (int iSubMethod = 0; iSubMethod < subMethodRunStates.Length; iSubMethod++)
            {
                subMethodRunStates[iSubMethod].ThreadStart = new ParameterizedThreadStart((o) =>
                {
                    // Copy the broker context to this new thread
                    BrokerContext.Current = currentBrokerContext;
                    System.Web.HttpContext.Current = currentHttpContext;
                    var subMethodRunState = subMethodRunStates[(int)o];

                    Mutex mutex = null;
                    
                    try
                    {
                        if (!string.IsNullOrEmpty(subMethodRunState.SubMethodInfo.LockKey))
                        {
                            mutex = new Mutex(false, subMethodRunState.SubMethodInfo.LockKey);
                            mutex.WaitOne();
                        }
                        // Loop over clearData providers until one succeeds
                        foreach (IDataProvider prov in subMethodRunState.DataProviders)
                        {
                            try
                            {
                                object subResult = subMethodRunState.SubMethodInfo.Invoke(prov);
                                // See if result can be used to update local database
                                if (prov is IExternalDataProvider && subMethodRunState.SubMethodInfo.IsUpdatableOutput(subResult))
                                {
                                    try
                                    {
                                        subMethodRunState.SubMethodInfo.InvokeUpdateMethod(subResult);
                                    }
                                    catch (Exception updateException)
                                    {
                                        string xml = Strings.SerializeObject(subResult);
                                        Local.Admin.LogException(updateException);
                                    }
                                }
                                // Exit loop if succeeded
                                if (subMethodRunState.SubMethodInfo.IsSuccessfulOutput(subResult))
                                {
                                    subMethodRunState.Result = subResult;
                                    subMethodRunState.Succeeded = true;
                                    break;
                                }
                            }
                            catch (Exception dataProviderException)
                            {
                                Local.Admin.LogException(dataProviderException);
                            }
                        }
                    }
                    finally
                    {
                        if (mutex != null)
                        {
                            mutex.ReleaseMutex();
                            mutex.Dispose();
                        }
                    }

                    if (!subMethodRunState.Succeeded)
                    {
                        // TODO: Add something here to identify the input for which all data providers have failed
                        Local.Admin.AddNewLog(TraceEventType.Information, BrokerContext.Current.WebMethodMessageName, string.Format("{0}: {1}", TextMessages.AllDataProvidersFailed, subMethodRunState.SubMethodInfo.InputToString()), null, null);
                    }

                    // Signal the end of processing
                    Interlocked.Increment(ref finishedThreads);
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

                while (System.Threading.Interlocked.Read(ref finishedThreads) < subMethodRunStates.Length && (DateTime.Now - executionStartTime).TotalMilliseconds < ConfigManager.Current.Settings.DataProviderMillisecondsTimeout)
                {
                    int waitMilliseconds = 100;
                    Thread.Sleep(waitMilliseconds);
                }
                foreach (var mi in subMethodRunStates)
                {
                    mi.Thread.Abort();
                }
            }
            else if (subMethodRunStates.Length == 1)
            {
                subMethodRunStates[0].ThreadStart.Invoke(0);
            }
            #endregion
        }

        public TOutput AggregateResults<TSubMethod, TOutput, TMainOutputItem>(FacadeMethodInfo<TSubMethod, TOutput, TMainOutputItem> facade, SubMethodRunState<TSubMethod>[] subMethodRunStates)
            where TOutput : class, IBasicOutput<TMainOutputItem>, new()
            where TSubMethod : SubMethodInfo
        {
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
                    facade.SetMainItem(output, outputMainItem);
                    if (succeededCount == subMethodRunStates.Length)
                    {
                        output.StandardRetur = StandardReturType.OK();
                    }
                    else
                    {
                        var failedSubMethods = subMethodRunStates.Where(smi => !smi.Succeeded).Select(smi => smi.SubMethodInfo);
                        var failedSubMethodsByReason = failedSubMethods.GroupBy(smi => smi.PossibleErrorReason());
                        var failuresAndReasons = failedSubMethodsByReason.ToDictionary(grp => grp.Key, grp => grp.Select(smi => smi.InputToString()));
                        output.StandardRetur = StandardReturType.PartialSuccess(failuresAndReasons);
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
                // TODO: Is it possible to put details why each item has failed?
                return new TOutput() { StandardRetur = StandardReturType.Create(HttpErrorCode.DATASOURCE_UNAVAILABLE) };
            }
            #endregion
        }


        public TOutput GetBatchMethodOutput<TInterface, TOutput, TSingleInputItem, TSingleOutputItem>(BatchFacadeMethodInfo<TInterface, TOutput, TSingleInputItem, TSingleOutputItem> facade)
            where TInterface : class, IDataProvider
            where TOutput : class, IBasicOutput<TSingleOutputItem[]>, new()
        {
            try
            {
                StandardReturType standardRetur;
                SubMethodRunState<SubMethodInfo>[] subMethodRunStates;

                standardRetur = Validate<SubMethodInfo, TOutput, TSingleOutputItem[]>(facade);
                if (!StandardReturType.IsSucceeded(standardRetur))
                {
                    return new TOutput() { StandardRetur = standardRetur };
                }

                standardRetur = Initialize<SubMethodInfo, TOutput, TSingleOutputItem[]>(facade, out subMethodRunStates);
                if (!StandardReturType.IsSucceeded(standardRetur))
                {
                    return new TOutput() { StandardRetur = standardRetur };
                }

                return facade.Run(subMethodRunStates);
            }
            catch (Exception ex)
            {
                Local.Admin.LogException(ex);
                return new TOutput() { StandardRetur = StandardReturType.UnspecifiedError() };
            }
        }
    }

}
