﻿//------------------------------------------------------------------------------
// <auto-generated>
//     This code was generated by a tool.
//     Runtime Version:2.0.50727.3615
//
//     Changes to this file may cause incorrect behavior and will be lost if
//     the code is regenerated.
// </auto-generated>
//------------------------------------------------------------------------------

namespace CprBroker.Installers.EventBrokerInstallers.Properties {
    using System;
    
    
    /// <summary>
    ///   A strongly-typed resource class, for looking up localized strings, etc.
    /// </summary>
    // This class was auto-generated by the StronglyTypedResourceBuilder
    // class via a tool like ResGen or Visual Studio.
    // To add or remove a member, edit your .ResX file then rerun ResGen
    // with the /str option, or rebuild your VS project.
    [global::System.CodeDom.Compiler.GeneratedCodeAttribute("System.Resources.Tools.StronglyTypedResourceBuilder", "2.0.0.0")]
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
                    global::System.Resources.ResourceManager temp = new global::System.Resources.ResourceManager("CprBroker.Installers.EventBrokerInstallers.Properties.Resources", typeof(Resources).Assembly);
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
        ///   Looks up a localized string similar to ChannelTypeId;Name
        ///1;WebService
        ///2;GPAC
        ///3;FileShare
        ///.
        /// </summary>
        internal static string ChannelType {
            get {
                return ResourceManager.GetString("ChannelType", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to /****** Object:  ForeignKey [FK_BirthdateEventNotification_EventNotification]    Script Date: 02/13/2011 17:59:04 ******/
        ///IF  EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N&apos;[dbo].[FK_BirthdateEventNotification_EventNotification]&apos;) AND parent_object_id = OBJECT_ID(N&apos;[dbo].[BirthdateEventNotification]&apos;))
        ///ALTER TABLE [dbo].[BirthdateEventNotification] DROP CONSTRAINT [FK_BirthdateEventNotification_EventNotification]
        ///GO
        ////****** Object:  ForeignKey [FK_BirthdateNotification_Notification [rest of string was truncated]&quot;;.
        /// </summary>
        internal static string CreateEventBrokerDatabaseObjects {
            get {
                return ResourceManager.GetString("CreateEventBrokerDatabaseObjects", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to SubscriptionTypeId;TypeName
        ///1;ChangeSubscription
        ///2;BirthdateSubscription
        ///.
        /// </summary>
        internal static string SubscriptionType {
            get {
                return ResourceManager.GetString("SubscriptionType", resourceCulture);
            }
        }
    }
}
