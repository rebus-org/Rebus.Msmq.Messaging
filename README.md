# Rebus.Msmq.Messaging

[![install from nuget](https://img.shields.io/nuget/v/Rebus.Msmq.Messaging.svg?style=flat-square)](https://www.nuget.org/packages/Rebus.Msmq.Messaging)

Provides a MSMQ transport for [Rebus](https://github.com/rebus-org/Rebus) based on the [MSMQ.Messaging](https://www.nuget.org/packages/MSMQ.Messaging/) NuGet package.

![](https://raw.githubusercontent.com/rebus-org/Rebus/master/artwork/little_rebusbus2_copy-200x200.png)

---

NOTE THAT THIS PACKAGE IS BASED ON [MSMQ.Messaging](https://www.nuget.org/packages/MSMQ.Messaging/), which is NOT Microsoft's official MSMQ driver.

🤠

The reason for this package to exist, is because Microsoft chose to abandon the entire `System.Messaging` namespace when they ported code to .NET Core.

You can read more in [this GitHub issue: 'Add support for SystemMessaging and SOAP Web Services'](https://github.com/dotnet/runtime/issues/16409).


### Are there differences between this transport and [Rebus' real MSMQ transport](https://github.com/rebus-org/Rebus.Msmq)?

YES! This package is based on an experimental port of the code from full FX `System.Messaging`, which is residing here: [MSMQ.Messaging on GitHub](https://github.com/weloytty/MSMQ.Messaging).

This means that it might not work as expected. Or maybe it does? The tests seem to pass. All I'm saying is this: USE AT YOUR OWN RISK! 😨

✨✨✨ October 2024 note: I have now had first-hand experience using the Rebus.Msmq.Messaging library in production systems for almost a year, and there hasn't been any issues with it, whatsoever! ✨✨✨
