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
using System.ComponentModel;
using System.Diagnostics;
using System.Linq;
using System.Text;
using CprBroker.Engine;
using CprBroker.Utilities;

namespace CprBroker.Engine.Tasks
{
    /// <summary>
    /// Base class for running a task once whenever a period passes
    /// </summary>
    public partial class PeriodicTaskExecuter : Component
    {
        // TODO: Shall SyncObject be defined per each task class?
        private System.Threading.AutoResetEvent SyncObject = new System.Threading.AutoResetEvent(true);

        public bool LogTimerEvents { get; set; }
        public int BatchSize { get; set; }

        public TimeSpan TimerInterval
        {
            get { return TimeSpan.FromMilliseconds(ActionTimer.Interval); }
            set { ActionTimer.Interval = value.TotalMilliseconds; }
        }

        public PeriodicTaskExecuter()
        {
            InitializeComponent();
            InitializeTimer();
            this.Disposed += new EventHandler(PeriodicTaskExecuter_Disposed);
        }

        public PeriodicTaskExecuter(IContainer container)
        {
            container.Add(this);
            InitializeComponent();
            InitializeTimer();
            this.Disposed += new EventHandler(PeriodicTaskExecuter_Disposed);
        }

        void PeriodicTaskExecuter_Disposed(object sender, EventArgs e)
        {
            if (SyncObject != null)
            {
                (SyncObject as IDisposable).Dispose();
            }
            if (ActionTimer != null)
                ActionTimer.Elapsed -= new System.Timers.ElapsedEventHandler(ActionTimer_Elapsed);
        }

        private void InitializeTimer()
        {
            ActionTimer.AutoReset = true;
            ActionTimer.Elapsed += new System.Timers.ElapsedEventHandler(ActionTimer_Elapsed);
        }

        protected virtual void OnBeforeStart()
        {
        }

        public void Start()
        {
            CprBroker.Engine.Local.Admin.LogSuccess(string.Format("{0} : <{1}>, running every <{2}>, batch size <{3}>",
                TextMessages.TaskStaring,
                this.GetType(),
                this.TimerInterval,
                this.BatchSize
                ));
            OnBeforeStart();
            ActionTimer.Start();
        }

        public virtual void OnAfterStop()
        {
        }

        public void Stop()
        {
            ActionTimer.Stop();
            OnAfterStop();
        }

        void ActionTimer_Elapsed(object sender, System.Timers.ElapsedEventArgs e)
        {
            if (SyncObject.WaitOne(1))
            {
                try
                {
                    BrokerContext.Initialize(Constants.EventBrokerApplicationToken.ToString(), Constants.UserToken);
                    if (LogTimerEvents)
                        CprBroker.Engine.Local.Admin.LogSuccess(string.Format("{0} : {1}", TextMessages.TimerEventStarted, this.GetType()));

                    try
                    {
                        PerformTimerAction();
                    }
                    catch (Exception ex)
                    {
                        CprBroker.Engine.Local.Admin.LogException(ex);
                    }

                    if (LogTimerEvents)
                        CprBroker.Engine.Local.Admin.LogSuccess(string.Format("{0} : {1}", TextMessages.TimerEventFinished, this.GetType()));
                }
                finally
                {
                    SyncObject.Set();
                }
            }
        }

        protected virtual void PerformTimerAction()
        {

        }

    }
}
