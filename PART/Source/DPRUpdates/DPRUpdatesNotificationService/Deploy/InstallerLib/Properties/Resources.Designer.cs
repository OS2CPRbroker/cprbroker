﻿//------------------------------------------------------------------------------
// <auto-generated>
//     This code was generated by a tool.
//     Runtime Version:4.0.30319.225
//
//     Changes to this file may cause incorrect behavior and will be lost if
//     the code is regenerated.
// </auto-generated>
//------------------------------------------------------------------------------

namespace InstallerLib.Properties {
    using System;
    
    
    /// <summary>
    ///   A strongly-typed resource class, for looking up localized strings, etc.
    /// </summary>
    // This class was auto-generated by the StronglyTypedResourceBuilder
    // class via a tool like ResGen or Visual Studio.
    // To add or remove a member, edit your .ResX file then rerun ResGen
    // with the /str option, or rebuild your VS project.
    [global::System.CodeDom.Compiler.GeneratedCodeAttribute("System.Resources.Tools.StronglyTypedResourceBuilder", "4.0.0.0")]
    [global::System.Diagnostics.DebuggerNonUserCodeAttribute()]
    [global::System.Runtime.CompilerServices.CompilerGeneratedAttribute()]
    internal class Resources {
        
        private static global::System.Resources.ResourceManager resourceMan;
        
        private static global::System.Globalization.CultureInfo resourceCulture;
        
        [global::System.Diagnostics.CodeAnalysis.SuppressMessageAttribute("Microsoft.Performance", "CA1811:AvoidUncalledPrivateCode")]
        internal Resources() {
        }
        
        /// <summary>
        ///   Returns the cached ResourceManager instance used by this class.
        /// </summary>
        [global::System.ComponentModel.EditorBrowsableAttribute(global::System.ComponentModel.EditorBrowsableState.Advanced)]
        internal static global::System.Resources.ResourceManager ResourceManager {
            get {
                if (object.ReferenceEquals(resourceMan, null)) {
                    global::System.Resources.ResourceManager temp = new global::System.Resources.ResourceManager("InstallerLib.Properties.Resources", typeof(Resources).Assembly);
                    resourceMan = temp;
                }
                return resourceMan;
            }
        }
        
        /// <summary>
        ///   Overrides the current thread's CurrentUICulture property for all
        ///   resource lookups using this strongly typed resource class.
        /// </summary>
        [global::System.ComponentModel.EditorBrowsableAttribute(global::System.ComponentModel.EditorBrowsableState.Advanced)]
        internal static global::System.Globalization.CultureInfo Culture {
            get {
                return resourceCulture;
            }
            set {
                resourceCulture = value;
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to /*==============================================================*/
        ////* DBMS name:      Microsoft SQL Server 2005                    */
        ////* Created on:     23-03-2011 07:18:47                          */
        ////*==============================================================*/
        ///
        ///
        ///if exists (select 1
        ///          from sysobjects
        ///          where  id = object_id(&apos;fnGK_DPRUpdateStaging_Get_TriggerNamePrefix&apos;)
        ///          and type in (&apos;IF&apos;, &apos;FN&apos;, &apos;TF&apos;))
        ///   drop function fnGK_DPRUpdateStaging_Get_TriggerNamePrefix
        ///go        /// [rest of string was truncated]&quot;;.
        /// </summary>
        internal static string crebas {
            get {
                return ResourceManager.GetString("crebas", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to /*==============================================================*/
        ////* DBMS name:      Microsoft SQL Server 2005                    */
        ////* Created on:     23-03-2011 07:18:47                          */
        ////*==============================================================*/
        ///
        ///
        ///if exists (select 1
        ///          from sysobjects
        ///          where  id = object_id(&apos;fnGK_DPRUpdateStaging_Get_TriggerNamePrefix&apos;)
        ///          and type in (&apos;IF&apos;, &apos;FN&apos;, &apos;TF&apos;))
        ///   drop function fnGK_DPRUpdateStaging_Get_TriggerNamePrefix
        ///go        /// [rest of string was truncated]&quot;;.
        /// </summary>
        internal static string drpbas {
            get {
                return ResourceManager.GetString("drpbas", resourceCulture);
            }
        }
    }
}
