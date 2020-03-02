/* ***** BEGIN LICENSE BLOCK *****
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
 * Dennis Isaksen
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
using CprBroker.Engine;
using CprBroker.Schemas;
using CprBroker.Schemas.Part;
using CprBroker.Utilities;
using System.Net.Sockets;
using CprBroker.Engine.Local;
using CprBroker.PartInterface;

namespace CprBroker.Providers.CPRDirect
{
    public partial class CPRDirectClientDataProvider
    {
        public IndividualResponseType GetResponse(IndividualRequestType request)
        {
            string response;
            string error;
            string operation = string.Format("{0}-{1}{2}", Constants.OnlineOperationName, request.DataType, request.SubscriptionType);
            if (Send(operation, request.PNR, request.Contents, out response, out error))
            {
                var ret = new IndividualResponseType() { Contents = response };
                ret.FillFromFixedLengthString(ret.Data, Constants.DataObjectMap);
                ret.SourceObject = ret.Data;
                return ret;
            }
            else
            {
                throw new Exception(error);
            }
        }

        protected bool Send(string operation, decimal pnr, string message, out string response, out string error)
        {
            // Force Sleep to avoid d.o.s. when doing batch operations.
            System.Threading.Thread.Sleep(500);

            // Used as citizen reference in log.
            PartManager pm = new PartManager();
            BrokerContext bc = BrokerContext.Current;
            GetUuidOutputType UUIDOutput = pm.GetUuid(bc.UserToken.ToString(), bc.ApplicationToken.ToString(), pnr.ToPnrDecimalString());
            string cprBrokerUUID = UUIDOutput.UUID;

            error = null;
            NetworkStream stream = null;

            using (var callContext = this.BeginCall(operation, pnr.ToPnrDecimalString()))
            {
                try
                {
                    TcpClient client = new TcpClient(Address, Port);
                    Byte[] data = Constants.TcpClientEncoding.GetBytes(message);

                    stream = client.GetStream();
                    stream.Write(data, 0, data.Length);

                    data = new Byte[Constants.ResponseLengths.MaxResponseLength];

                    int bytes = stream.Read(data, 0, data.Length);
                    response = Constants.TcpClientEncoding.GetString(data, 0, bytes);

                    string errorCode = response.Substring(Constants.ResponseLengths.ErrorCodeIndex, Constants.ResponseLengths.ErrorCodeLength);

                    // "00" is code for "NO ERROR".
                    // Reference: "CPR Direkte Grænsefladebeskrivelse OFF4.pdf"(v7.0), p.11.
                    if (errorCode == "00")
                    {
                        // We log the call and set the success parameter to true
                        callContext.Succeed();
                        Admin.LogFormattedSuccess("CPR Direct Proxy Client: CPR Broker UUID <{0}>, status code <{1}>", cprBrokerUUID, errorCode);
                        return true;                
                    }
                    else 
                    {
                        if (Constants.ErrorCodes.ContainsKey(errorCode))
                        {
                            error = Constants.ErrorCodes[errorCode];
                            Admin.LogFormattedSuccess("CPR Direct Proxy Client: CPR Broker UUID <{0}>, error code <{1}>", cprBrokerUUID, errorCode);
                        }
                        else
                        {
                            error = string.Format("An unkown CPR Direct error has occured: {0}", error);
                            Admin.LogFormattedSuccess("CPR Direct Proxy Client: CPR Broker UUID <{0}>, error code <{1}>", cprBrokerUUID, errorCode);
                        }
                        // We log the call and set the success parameter to false
                        callContext.Fail();
                        return false;
                    }
                }
                catch (Exception ex)
                {
                    // TODO: Shall we also log a call to the data provider?
                    callContext.Fail();
                    Admin.LogFormattedError("CPR client is not reachable on <{0}>:<{1}>, Error = <{2}>", Address, Port, ex.ToString());
                    response = null;
                    return false;
                }
            }
        }
    }
}
