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
using CprBroker.Schemas.Part;
using CprBroker.Schemas.Wrappers;

namespace CprBroker.Providers.CPRDirect
{
    public partial class IndividualResponseType
    {
        /// <summary>
        /// This method is only used by unit tests
        /// </summary>
        /// <param name="batchFileText"></param>
        /// <returns></returns>
        public static IList<IndividualResponseType> ParseBatch(string batchFileText)
        {
            var lines = LineWrapper.ParseBatch(batchFileText);
            return ParseBatch(lines, Constants.DataObjectMap);
        }

        /// <summary>
        /// This method is only used by unit tests
        /// </summary>
        /// <param name="batchFileText"></param>
        /// <returns></returns>
        public static IList<IndividualResponseType> ParseBatch(LineWrapper[] dataLines, Dictionary<string, Type> typeMap)
        {
            var startRecord = dataLines.First().ToWrapper(typeMap) as StartRecordType;
            var endRecord = dataLines.Last().ToWrapper(typeMap) as EndRecordType;

            dataLines = dataLines.Skip(1).Take(dataLines.Length - 2).ToArray();

            var groupedWrapers = dataLines
                            .GroupBy(w => w.PNR)
                            .ToList();

            var ret = groupedWrapers
                .Select(individualWrappersGrouping =>
                    {
                        var individual = new IndividualResponseType();
                        var individualLines = individualWrappersGrouping
                            .Select(il => il.ToPersonRecordWrapper(Constants.DataObjectMap, individual) as Wrapper)
                            .Where(w => w != null)
                            .ToList();
                        individual.FillPropertiesFromWrappers(individualLines, startRecord, endRecord);
                        return individual;
                    })
                .ToList();
            return ret;
        }



    }
}

