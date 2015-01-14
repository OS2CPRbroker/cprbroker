﻿<%@ Control Language="C#" AutoEventWireup="true" CodeBehind="MessageDisplayer.ascx.cs" Inherits="CprBroker.Web.Pages.Controls.MessageDisplayer" %>

<script type="text/javascript">
    <!-- Script to show messages -->
    function openDialog(dialog) {
        var div = document.getElementById(dialog);
        if (div.style.display !== 'none') {
            div.style.display = 'none';
        }
        else {
            div.style.display = 'block';
        }
    };
</script>

<!-- Begin Messages -->
<div id="dialog-box" style="display: none;">
    <div class="dialog-box-inner">
        <div runat="server" enableviewstate="false" id="divMessages"></div>
        <button onclick="openDialog('dialog-box')" class="close">Close</button>
    </div>
</div>
<!-- End Messages -->
