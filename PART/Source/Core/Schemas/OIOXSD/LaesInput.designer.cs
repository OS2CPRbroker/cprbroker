//------------------------------------------------------------------------------
// <auto-generated>
//     This code was generated by a tool.
//     Runtime Version:4.0.30319.18408
//
//     Changes to this file may cause incorrect behavior and will be lost if
//     the code is regenerated.
// </auto-generated>
//------------------------------------------------------------------------------

// 
// This source code was auto-generated by xsd, Version=2.0.50727.3038.
// 
namespace CprBroker.Schemas.Part {
    using System.Xml.Serialization;
    
    
    /// <remarks/>
    [System.CodeDom.Compiler.GeneratedCodeAttribute("System.Xml", "4.0.30319.18408")]
    [System.SerializableAttribute()]
    [System.Diagnostics.DebuggerStepThroughAttribute()]
    [System.ComponentModel.DesignerCategoryAttribute("code")]
    [System.Xml.Serialization.XmlTypeAttribute(Namespace="urn:oio:sagdok:2.0.0")]
    [System.Xml.Serialization.XmlRootAttribute("LaesInput", Namespace="urn:oio:sagdok:2.0.0", IsNullable=false)]
    public partial class LaesInputType {
        
        private string uUIDField;
        
        private TidspunktType virkningFraFilterField;
        
        private TidspunktType virkningTilFilterField;
        
        private TidspunktType registreringFraFilterField;
        
        private TidspunktType registreringTilFilterField;
        
        /// <remarks/>
        [System.Xml.Serialization.XmlElementAttribute(Namespace="urn:oio:dkal:1.0.0")]
        public string UUID {
            get {
                return this.uUIDField;
            }
            set {
                this.uUIDField = value;
            }
        }
        
        /// <remarks/>
        public TidspunktType VirkningFraFilter {
            get {
                return this.virkningFraFilterField;
            }
            set {
                this.virkningFraFilterField = value;
            }
        }
        
        /// <remarks/>
        public TidspunktType VirkningTilFilter {
            get {
                return this.virkningTilFilterField;
            }
            set {
                this.virkningTilFilterField = value;
            }
        }
        
        /// <remarks/>
        public TidspunktType RegistreringFraFilter {
            get {
                return this.registreringFraFilterField;
            }
            set {
                this.registreringFraFilterField = value;
            }
        }
        
        /// <remarks/>
        public TidspunktType RegistreringTilFilter {
            get {
                return this.registreringTilFilterField;
            }
            set {
                this.registreringTilFilterField = value;
            }
        }
    }
}