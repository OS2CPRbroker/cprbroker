﻿//------------------------------------------------------------------------------
// <auto-generated>
//     This code was generated by a tool.
//     Runtime Version:2.0.50727.3615
//
//     Changes to this file may cause incorrect behavior and will be lost if
//     the code is regenerated.
// </auto-generated>
//------------------------------------------------------------------------------

// 
// This source code was auto-generated by Microsoft.VSDesigner, Version 2.0.50727.3615.
// 
#pragma warning disable 1591

namespace CprBroker.EventBroker.NotificationService {
    using System.Diagnostics;
    using System.Web.Services;
    using System.ComponentModel;
    using System.Web.Services.Protocols;
    using System;
    using System.Xml.Serialization;
    
    
    /// <remarks/>
    [System.CodeDom.Compiler.GeneratedCodeAttribute("System.Web.Services", "2.0.50727.3053")]
    [System.Diagnostics.DebuggerStepThroughAttribute()]
    [System.ComponentModel.DesignerCategoryAttribute("code")]
    [System.Web.Services.WebServiceBindingAttribute(Name="NotificationSoap", Namespace="http://tempuri.org/")]
    [System.Xml.Serialization.XmlIncludeAttribute(typeof(object[]))]
    public partial class Notification : System.Web.Services.Protocols.SoapHttpClientProtocol {
        
        private System.Threading.SendOrPostCallback NotifyOperationCompleted;
        
        private System.Threading.SendOrPostCallback PingOperationCompleted;
        
        private bool useDefaultCredentialsSetExplicitly;
        
        /// <remarks/>
        public Notification() {
            this.Url = global::CprBroker.EventBroker.Properties.Settings.Default.CprBroker_EventBroker_NotificationService_Notification;
            if ((this.IsLocalFileSystemWebService(this.Url) == true)) {
                this.UseDefaultCredentials = true;
                this.useDefaultCredentialsSetExplicitly = false;
            }
            else {
                this.useDefaultCredentialsSetExplicitly = true;
            }
        }
        
        public new string Url {
            get {
                return base.Url;
            }
            set {
                if ((((this.IsLocalFileSystemWebService(base.Url) == true) 
                            && (this.useDefaultCredentialsSetExplicitly == false)) 
                            && (this.IsLocalFileSystemWebService(value) == false))) {
                    base.UseDefaultCredentials = false;
                }
                base.Url = value;
            }
        }
        
        public new bool UseDefaultCredentials {
            get {
                return base.UseDefaultCredentials;
            }
            set {
                base.UseDefaultCredentials = value;
                this.useDefaultCredentialsSetExplicitly = true;
            }
        }
        
        /// <remarks/>
        public event NotifyCompletedEventHandler NotifyCompleted;
        
        /// <remarks/>
        public event PingCompletedEventHandler PingCompleted;
        
        /// <remarks/>
        [System.Web.Services.Protocols.SoapDocumentMethodAttribute("http://tempuri.org/Notify", RequestNamespace="http://tempuri.org/", ResponseNamespace="http://tempuri.org/", Use=System.Web.Services.Description.SoapBindingUse.Literal, ParameterStyle=System.Web.Services.Protocols.SoapParameterStyle.Wrapped)]
        public void Notify(string appToken, CommonEventStructureType notification) {
            this.Invoke("Notify", new object[] {
                        appToken,
                        notification});
        }
        
        /// <remarks/>
        public void NotifyAsync(string appToken, CommonEventStructureType notification) {
            this.NotifyAsync(appToken, notification, null);
        }
        
        /// <remarks/>
        public void NotifyAsync(string appToken, CommonEventStructureType notification, object userState) {
            if ((this.NotifyOperationCompleted == null)) {
                this.NotifyOperationCompleted = new System.Threading.SendOrPostCallback(this.OnNotifyOperationCompleted);
            }
            this.InvokeAsync("Notify", new object[] {
                        appToken,
                        notification}, this.NotifyOperationCompleted, userState);
        }
        
        private void OnNotifyOperationCompleted(object arg) {
            if ((this.NotifyCompleted != null)) {
                System.Web.Services.Protocols.InvokeCompletedEventArgs invokeArgs = ((System.Web.Services.Protocols.InvokeCompletedEventArgs)(arg));
                this.NotifyCompleted(this, new System.ComponentModel.AsyncCompletedEventArgs(invokeArgs.Error, invokeArgs.Cancelled, invokeArgs.UserState));
            }
        }
        
        /// <remarks/>
        [System.Web.Services.Protocols.SoapDocumentMethodAttribute("http://tempuri.org/Ping", RequestNamespace="http://tempuri.org/", ResponseNamespace="http://tempuri.org/", Use=System.Web.Services.Description.SoapBindingUse.Literal, ParameterStyle=System.Web.Services.Protocols.SoapParameterStyle.Wrapped)]
        public void Ping() {
            this.Invoke("Ping", new object[0]);
        }
        
        /// <remarks/>
        public void PingAsync() {
            this.PingAsync(null);
        }
        
        /// <remarks/>
        public void PingAsync(object userState) {
            if ((this.PingOperationCompleted == null)) {
                this.PingOperationCompleted = new System.Threading.SendOrPostCallback(this.OnPingOperationCompleted);
            }
            this.InvokeAsync("Ping", new object[0], this.PingOperationCompleted, userState);
        }
        
        private void OnPingOperationCompleted(object arg) {
            if ((this.PingCompleted != null)) {
                System.Web.Services.Protocols.InvokeCompletedEventArgs invokeArgs = ((System.Web.Services.Protocols.InvokeCompletedEventArgs)(arg));
                this.PingCompleted(this, new System.ComponentModel.AsyncCompletedEventArgs(invokeArgs.Error, invokeArgs.Cancelled, invokeArgs.UserState));
            }
        }
        
        /// <remarks/>
        public new void CancelAsync(object userState) {
            base.CancelAsync(userState);
        }
        
        private bool IsLocalFileSystemWebService(string url) {
            if (((url == null) 
                        || (url == string.Empty))) {
                return false;
            }
            System.Uri wsUri = new System.Uri(url);
            if (((wsUri.Port >= 1024) 
                        && (string.Compare(wsUri.Host, "localHost", System.StringComparison.OrdinalIgnoreCase) == 0))) {
                return true;
            }
            return false;
        }
    }
    
    /// <remarks/>
    [System.CodeDom.Compiler.GeneratedCodeAttribute("System.Xml", "2.0.50727.3082")]
    [System.SerializableAttribute()]
    [System.Diagnostics.DebuggerStepThroughAttribute()]
    [System.ComponentModel.DesignerCategoryAttribute("code")]
    [System.Xml.Serialization.XmlTypeAttribute(Namespace="http://tempuri.org/")]
    public partial class CommonEventStructureType {
        
        private EventInfoStructureType eventInfoStructureField;
        
        private string eventTopicField;
        
        private string eventSubscriptionReferenceField;
        
        private ExtensionStructureType extensionStructureField;
        
        private EventDetailStructureType eventDetailStructureField;
        
        /// <remarks/>
        public EventInfoStructureType EventInfoStructure {
            get {
                return this.eventInfoStructureField;
            }
            set {
                this.eventInfoStructureField = value;
            }
        }
        
        /// <remarks/>
        public string EventTopic {
            get {
                return this.eventTopicField;
            }
            set {
                this.eventTopicField = value;
            }
        }
        
        /// <remarks/>
        public string EventSubscriptionReference {
            get {
                return this.eventSubscriptionReferenceField;
            }
            set {
                this.eventSubscriptionReferenceField = value;
            }
        }
        
        /// <remarks/>
        public ExtensionStructureType ExtensionStructure {
            get {
                return this.extensionStructureField;
            }
            set {
                this.extensionStructureField = value;
            }
        }
        
        /// <remarks/>
        public EventDetailStructureType EventDetailStructure {
            get {
                return this.eventDetailStructureField;
            }
            set {
                this.eventDetailStructureField = value;
            }
        }
    }
    
    /// <remarks/>
    [System.CodeDom.Compiler.GeneratedCodeAttribute("System.Xml", "2.0.50727.3082")]
    [System.SerializableAttribute()]
    [System.Diagnostics.DebuggerStepThroughAttribute()]
    [System.ComponentModel.DesignerCategoryAttribute("code")]
    [System.Xml.Serialization.XmlTypeAttribute(Namespace="http://tempuri.org/")]
    public partial class EventInfoStructureType {
        
        private string eventIdentifierField;
        
        private string eventProducerReferenceField;
        
        private System.DateTime eventRegistrationDateTimeField;
        
        private EventObjectStructureType eventObjectStructureField;
        
        private ExtensionStructureType extensionStructureField;
        
        /// <remarks/>
        public string EventIdentifier {
            get {
                return this.eventIdentifierField;
            }
            set {
                this.eventIdentifierField = value;
            }
        }
        
        /// <remarks/>
        public string EventProducerReference {
            get {
                return this.eventProducerReferenceField;
            }
            set {
                this.eventProducerReferenceField = value;
            }
        }
        
        /// <remarks/>
        public System.DateTime EventRegistrationDateTime {
            get {
                return this.eventRegistrationDateTimeField;
            }
            set {
                this.eventRegistrationDateTimeField = value;
            }
        }
        
        /// <remarks/>
        public EventObjectStructureType EventObjectStructure {
            get {
                return this.eventObjectStructureField;
            }
            set {
                this.eventObjectStructureField = value;
            }
        }
        
        /// <remarks/>
        public ExtensionStructureType ExtensionStructure {
            get {
                return this.extensionStructureField;
            }
            set {
                this.extensionStructureField = value;
            }
        }
    }
    
    /// <remarks/>
    [System.CodeDom.Compiler.GeneratedCodeAttribute("System.Xml", "2.0.50727.3082")]
    [System.SerializableAttribute()]
    [System.Diagnostics.DebuggerStepThroughAttribute()]
    [System.ComponentModel.DesignerCategoryAttribute("code")]
    [System.Xml.Serialization.XmlTypeAttribute(Namespace="http://tempuri.org/")]
    public partial class EventObjectStructureType {
        
        private string objectTypeReferenceField;
        
        private string eventObjectReferenceField;
        
        private string actionSchemeReferenceField;
        
        private string eventObjectActionCodeField;
        
        /// <remarks/>
        public string ObjectTypeReference {
            get {
                return this.objectTypeReferenceField;
            }
            set {
                this.objectTypeReferenceField = value;
            }
        }
        
        /// <remarks/>
        public string EventObjectReference {
            get {
                return this.eventObjectReferenceField;
            }
            set {
                this.eventObjectReferenceField = value;
            }
        }
        
        /// <remarks/>
        public string actionSchemeReference {
            get {
                return this.actionSchemeReferenceField;
            }
            set {
                this.actionSchemeReferenceField = value;
            }
        }
        
        /// <remarks/>
        public string EventObjectActionCode {
            get {
                return this.eventObjectActionCodeField;
            }
            set {
                this.eventObjectActionCodeField = value;
            }
        }
    }
    
    /// <remarks/>
    [System.CodeDom.Compiler.GeneratedCodeAttribute("System.Xml", "2.0.50727.3082")]
    [System.SerializableAttribute()]
    [System.Diagnostics.DebuggerStepThroughAttribute()]
    [System.ComponentModel.DesignerCategoryAttribute("code")]
    [System.Xml.Serialization.XmlTypeAttribute(Namespace="http://tempuri.org/")]
    public abstract partial class EventDetailStructureType {
    }
    
    /// <remarks/>
    [System.CodeDom.Compiler.GeneratedCodeAttribute("System.Xml", "2.0.50727.3082")]
    [System.SerializableAttribute()]
    [System.Diagnostics.DebuggerStepThroughAttribute()]
    [System.ComponentModel.DesignerCategoryAttribute("code")]
    [System.Xml.Serialization.XmlTypeAttribute(Namespace="http://tempuri.org/")]
    public partial class ExtensionStructureType {
        
        private System.Guid idField;
        
        private object[] itemField;
        
        /// <remarks/>
        public System.Guid ID {
            get {
                return this.idField;
            }
            set {
                this.idField = value;
            }
        }
        
        /// <remarks/>
        public object[] Item {
            get {
                return this.itemField;
            }
            set {
                this.itemField = value;
            }
        }
    }
    
    /// <remarks/>
    [System.CodeDom.Compiler.GeneratedCodeAttribute("System.Web.Services", "2.0.50727.3053")]
    public delegate void NotifyCompletedEventHandler(object sender, System.ComponentModel.AsyncCompletedEventArgs e);
    
    /// <remarks/>
    [System.CodeDom.Compiler.GeneratedCodeAttribute("System.Web.Services", "2.0.50727.3053")]
    public delegate void PingCompletedEventHandler(object sender, System.ComponentModel.AsyncCompletedEventArgs e);
}

#pragma warning restore 1591