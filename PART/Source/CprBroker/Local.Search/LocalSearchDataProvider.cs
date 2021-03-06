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
using System.Linq.Expressions;

using CprBroker.Engine;
using CprBroker.Utilities;
using CprBroker.Schemas;
using CprBroker.Schemas.Part;

namespace CprBroker.Providers.Local.Search
{
    public class LocalSearchDataProvider : IPartSearchDataProvider, IPartSearchListDataProvider
    {

        public Guid[] Search(CprBroker.Schemas.Part.SoegInputType1 searchCriteria)
        {
            using (var dataContext = new PartSearchDataContext())
            {
                int firstResults = 0;
                int.TryParse(searchCriteria.FoersteResultatReference, out firstResults);

                int maxResults = 0;
                int.TryParse(searchCriteria.MaksimalAntalKvantitet, out maxResults);
                if (maxResults <= 0)
                {
                    maxResults = 1000;
                }

                var expr = CreateWhereExpression(dataContext, searchCriteria);
                return dataContext
                    .PersonSearchCaches
                    .Where(expr)
                    .OrderBy(psc => psc.UUID)
                    .Select(psc => psc.UUID)
                    .Skip(firstResults)
                    .Take(maxResults)
                    .ToArray();
            }
        }

        public LaesResultatType[] SearchList(Schemas.Part.SoegInputType1 searchCriteria)
        {
            using (var dataContext = new PartSearchDataContext())
            {
                #region Search
                int firstResults = 0;
                int.TryParse(searchCriteria.FoersteResultatReference, out firstResults);

                int maxResults = 0;
                int.TryParse(searchCriteria.MaksimalAntalKvantitet, out maxResults);
                if (maxResults <= 0)
                {
                    maxResults = 1000;
                }

                var expr = CreateWhereExpression(dataContext, searchCriteria);
                var ids = dataContext
                    .PersonSearchCaches
                    .Where(expr)
                    .OrderBy(psc => psc.UUID)
                    .Select(psc => new { UUID = psc.UUID, psc.PersonRegistrationId })
                    .Skip(firstResults)
                    .Take(maxResults)
                    .ToArray();

                var uuids = ids.Select(uuid => uuid.UUID).ToArray();
                #endregion

                #region filling the result
                using (var partDataContext = new CprBroker.Data.Part.PartDataContext())
                {
                    var ret = new List<LaesResultatType>(uuids.Count());

                    var regs = partDataContext.PersonRegistrations
                        .Where(pr => uuids.Contains(pr.UUID))
                        .ToArray();

                    foreach (var id in ids)
                    {
                        LaesResultatType laesResultat = null;

                        var dbReg = regs.Where(r => r.PersonRegistrationId == id.PersonRegistrationId).SingleOrDefault();
                        if (dbReg != null)
                        {
                            var reg = CprBroker.Data.Part.PersonRegistration.ToXmlType(dbReg);

                            if (reg != null)
                            {
                                reg.FilterToLatestSnapshot();
                                laesResultat = new LaesResultatType()
                                {
                                    Item = new FiltreretOejebliksbilledeType()
                                    {
                                        AttributListe = reg.AttributListe,
                                        BrugervendtNoegleTekst = null,
                                        RelationListe = reg.RelationListe,
                                        UUID = null,
                                        TilstandListe = reg.TilstandListe
                                    }
                                };
                            }
                        }
                        ret.Add(laesResultat);
                    }
                    return ret.ToArray();
                }
                #endregion
            }
        }

        public static Expression<Func<PersonSearchCache, bool>> CreateWhereExpression(PartSearchDataContext dataContext, CprBroker.Schemas.Part.SoegInputType1 searchCriteria)
        {
            var pred = PredicateBuilder.True<PersonSearchCache>();

            if (searchCriteria.SoegObjekt != null)
            {
                pred = pred.And(searchCriteria.SoegObjekt);
            }

            return pred;
        }
        public bool IsAlive()
        {
            using (var dataContext = new PartSearchDataContext())
            {
                try
                {
                    var first = dataContext.PersonSearchCaches.FirstOrDefault();
                    return true;
                }
                catch (Exception ex)
                {
                    CprBroker.Engine.Local.Admin.LogException(ex);
                    return false;
                }
            }
        }

        public Version Version
        {
            get { return new Version(CprBroker.Utilities.Constants.Versioning.Major, CprBroker.Utilities.Constants.Versioning.Minor); }
        }

        public static void DeleteFromSearchCache(PersonIdentifier personIdentifier)
        {
            using (var searchDataContext = new PartSearchDataContext())
            {
                var personsToDelete = searchDataContext.PersonSearchCaches.Where(p => p.UUID == personIdentifier.UUID);
                searchDataContext.PersonSearchCaches.DeleteAllOnSubmit(personsToDelete);

                searchDataContext.SubmitChanges();
            }
        }
    }
}
