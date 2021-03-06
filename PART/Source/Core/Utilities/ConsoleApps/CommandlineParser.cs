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
using System.IO;

namespace CprBroker.Utilities.ConsoleApps
{
    class CommandlineParser
    {
        public static List<CommandArgument> SplitCommandArguments(string[] args)
        {
            var ret = new List<CommandArgument>();
            CommandArgument currentArgument = null;
            for (int i = 0; i < args.Length; i++)
            {
                var arg = args[i];
                if (arg.StartsWith("/"))
                {
                    currentArgument = new CommandArgument() { Switch = arg };
                    ret.Add(currentArgument);
                }
                else
                {
                    if (currentArgument == null)
                    {
                        currentArgument = new CommandArgument() { Value = arg };
                        ret.Add(currentArgument);
                    }
                    else
                    {
                        currentArgument.Value = arg;
                    }
                    currentArgument = null;
                }
            }
            return ret;
        }

        public static void ValidateCommandline(List<CommandArgument> parsedArguments, CommandArgumentSpec[] specs)
        {
            foreach (var spec in specs)
            {
                spec.FoundArguments = parsedArguments.Where(ca => string.Equals(spec.Switch, ca.Switch)).ToArray();
                if (spec.FoundArguments.Length < spec.MinOccurs)
                {
                    throw new Exception(string.Format("Argument not specified for {0}", spec.Description));
                }
                if (spec.FoundArguments.Length > spec.MaxOccurs)
                {
                    throw new Exception(string.Format("Too many arguments specified for {0}", spec.Description));
                }
                foreach (var fa in spec.FoundArguments)
                {
                    if (spec.ValueRequirement != ValueRequirement.NotRequired)
                    {
                        if (string.IsNullOrEmpty(fa.Value))
                        {
                            throw new Exception(string.Format("Missing argument for {0}", spec.Description));
                        }
                        if (spec.ValueRequirement == ValueRequirement.RequiredAndFileExists)
                        {
                            if (!File.Exists(fa.Value))
                            {
                                throw new FileNotFoundException(string.Format("File not found: {0}", fa.Value));
                            }
                            fa.Value = new FileInfo(fa.Value).FullName;
                        }
                    }
                }
                parsedArguments.RemoveAll(ca => spec.FoundArguments.Contains(ca));
            }
            if (parsedArguments.Count > 0)
            {
                throw new Exception(
                    string.Format(
                        "Unknown arguments {0}",
                        string.Join(
                            " ",
                            parsedArguments.Select(pa => pa.ToString()).ToArray()
                        )
                    ));
            }
        }

    }
}
