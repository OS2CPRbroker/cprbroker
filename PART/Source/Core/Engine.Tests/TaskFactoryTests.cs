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

using NUnit.Framework;
using CprBroker.Engine.Tasks;
using CprBroker.Utilities;
using CprBroker.Utilities.Config;

namespace CprBroker.Tests.Engine
{
    namespace TaskFactoryTests
    {
        [TestFixture]
        public class CreateTask
        {
            [Test]
            public void CreateTask_InvalidType_Null()
            {
                var f = new TaskFactory();
                var elm = new TasksConfigurationSection.TaskElement() { TypeName = "123" };
                var task = f.CreateTask<PeriodicTaskExecuter>(elm);
                Assert.Null(task);
            }

            [Test]
            public void CreateTask_GoodType_NotNull()
            {
                var f = new TaskFactory();
                var elm = new TasksConfigurationSection.TaskElement() { TypeName = typeof(PeriodicTaskExecuter).IdentifyableName() };
                var task = f.CreateTask<PeriodicTaskExecuter>(elm);
                Assert.NotNull(task);
            }
        }

        [TestFixture]
        public class LoadTasks
        {
            public class TaskFactoryStub : TaskFactory
            {
                public TasksConfigurationSection.TaskElement[] Elements;
                public override TasksConfigurationSection LoadTasksSection()
                {
                    var section = new TasksConfigurationSection() { };
                    foreach (var elm in this.Elements)
                        section.AutoLoaded.Add(elm);
                    return section;
                }
            }

            public string[] InvalidTypeNames
            {
                get
                {
                    return new string[] { 
                        "asfka;slkd",
                        typeof(object).IdentifyableName()
                    };
                }
            }
            [Test]
            public void LoadTasks_InvalidType_ZeroTasks([ValueSource("InvalidTypeNames")]string typeName)
            {
                var factory = new TaskFactoryStub() { Elements = new TasksConfigurationSection.TaskElement[] { new TasksConfigurationSection.TaskElement() { TypeName = typeName } } };
                var tasks = factory.LoadTasks();
                Assert.AreEqual(0, tasks.Length);
            }

            [Test]
            public void LoadTasks_InvalidType_ErrorRaised([ValueSource("InvalidTypeNames")]string typeName)
            {
                var factory = new TaskFactoryStub() { Elements = new TasksConfigurationSection.TaskElement[] { new TasksConfigurationSection.TaskElement() { TypeName = typeName } } };
                bool errorRaised = false;
                factory.TaskElementConfigError += (object sender, TaskFactory.ErrorEventArgs<TasksConfigurationSection.TaskElement> e) => errorRaised = true;

                var tasks = factory.LoadTasks();
                Assert.True(errorRaised);
            }

        }

    }
}
