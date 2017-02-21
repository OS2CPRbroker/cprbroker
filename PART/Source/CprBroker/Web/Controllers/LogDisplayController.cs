﻿using CprBroker.Data.Applications;
using System;
using System.Collections.Generic;
using System.Data.Linq;
using System.Linq;
using System.Web;
using System.Web.Mvc;

namespace CprBroker.Web.Controllers
{
    public class LogDisplayParameters
    {
        public DateTime? From;
        public DateTime? To;
        public int PageSize = 20;
        public int PageNumber = 0;

        public DateTime EffectiveFrom { get { return From ?? CprBroker.Utilities.Constants.MinSqlDate; } }
        public DateTime EffectiveTo { get { return To ?? CprBroker.Utilities.Constants.MaxSqlDate; } }
    }

    [RoutePrefix("Pages/LogDisplay")]
    public class LogDisplayController : Controller
    {
        
        [Route("")]
        public ActionResult Activities(LogDisplayParameters pars)
        {
            using (var dc = new ApplicationDataContext())
            {
                var loadOptions = new DataLoadOptions();
                loadOptions.LoadWith<Activity>(ac => ac.Application);
                dc.LoadOptions = loadOptions;
                var acts = dc.Activities.Where(a => a.StartTS >= pars.EffectiveFrom && a.StartTS <= pars.EffectiveTo)
                    .OrderByDescending(a => a.StartTS)
                    .Skip(pars.PageSize * pars.PageNumber)
                    .Take(pars.PageSize)
                    .ToArray();

                return View(acts);
            }
        }

        [Route("Details/{activityId}")]
        public ActionResult Activity(Guid activityId)
        {
            using (var dc = new ApplicationDataContext())
            {
                var act = dc.Activities.SingleOrDefault(a => a.ActivityId == activityId);
                return View("Activity", act);
            }
        }

        public ActionResult Operations(LogDisplayParameters pars)
        {
            using (var dc = new ApplicationDataContext())
            {
                var acts = dc.Operations.Where(a => a.Activity.StartTS >= pars.EffectiveFrom && a.Activity.StartTS <= pars.EffectiveTo)
                    .OrderByDescending(a => a.Activity.StartTS)
                    .Skip(pars.PageSize * pars.PageNumber)
                    .Take(pars.PageNumber)
                    .ToArray();

                return View(acts);
            }
        }

        public ActionResult ExternalCalls(LogDisplayParameters pars)
        {
            using (var dc = new ApplicationDataContext())
            {
                var calls = dc.DataProviderCalls.Where(a => a.CallTime >= pars.EffectiveFrom && a.CallTime <= pars.EffectiveTo)
                    .OrderByDescending(a => a.CallTime)
                    .Skip(pars.PageSize * pars.PageNumber)
                    .Take(pars.PageNumber)
                    .ToArray();

                return View(calls);
            }
        }

        public ActionResult Logs(LogDisplayParameters pars, object type)
        {
            return View();
        }


    }
}