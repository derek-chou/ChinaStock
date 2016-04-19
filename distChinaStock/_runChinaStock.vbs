Set objShell_1 = WScript.CreateObject("WScript.Shell")
objShell_1.CurrentDirectory = "ChinaStock1"
objShell_1.Exec "PUTTY.EXE -C -L 6379:localhost:6379 apex@60.250.174.213 -pw apex-8791"

Set objShell_2 = WScript.CreateObject("WScript.Shell")
'objShell_2.CurrentDirectory = "c:\ChinaStock1"
objShell_2.Run("runWriteShow2003.bat"), 0, True

Set objShell_3 = WScript.CreateObject("WScript.Shell")
objShell_3.CurrentDirectory = "..\ChinaStock2"
objShell_3.Run("runWriteSjshq.bat"), 0, True
