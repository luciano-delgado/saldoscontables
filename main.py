
from modelo import proceso
from tkinter import StringVar, Entry, Label
import tkinter as tk


if __name__=="__main__":
    
    root = tk.Tk()
    root.geometry('350x100')
    root.title('OyP - Saldos Contables v1.0.2')
    label_fecha = Label(root, text="Ingrese fecha AAAA/MM/DD") #fh_consulta = "2022/11/30"
    label_fecha.pack()
    fecha = StringVar()
    Ent1 = Entry(root, textvariable=fecha)
    Ent1.pack()
    boton_leer = tk.Button(root,text="Iniciar Proceso",command=lambda: proceso(fecha.get()),bg='lightgreen',font =('calibri', 12)) 
    boton_leer.pack()
    root.mainloop()