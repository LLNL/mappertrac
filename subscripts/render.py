#!/usr/bin/env python2.7
import vtk

def render(input, output):
    # try:
    #     v = vtk.vtkMesaRenderer()
    #     if myProcId > 0:
    #         _graphics_fact=vtk.vtkGraphicsFactory()
    #         _graphics_fact.SetOffScreenOnlyMode(1)
    #         _graphics_fact.SetUseMesaClasses(1)
    #         del _graphics_fact
    #     del v
    # except Exception as e:
    #     print("No mesa", e)

    # create a rendering window and renderer
    ren = vtk.vtkRenderer()
    renWin = vtk.vtkRenderWindow()
    renWin.AddRenderer(ren)

    # create a renderwindowinteractor
    # iren = vtk.vtkRenderWindowInteractor()
    # iren.SetRenderWindow(renWin)

    cone = vtk.vtkConeSource()
    cone.SetHeight(3.0)
    cone.SetRadius(1.0)
    cone.SetResolution(10)

    coneMapper = vtk.vtkPolyDataMapper()
    coneMapper.SetInputConnection(cone.GetOutputPort())

    coneActor = vtk.vtkActor()
    coneActor.SetMapper(coneMapper)

    # assign actor to the renderer
    ren.AddActor(coneActor)

    renWin.Render()

    # screenshot code:
    w2if = vtk.vtkWindowToImageFilter()
    w2if.SetInput(renWin)
    w2if.Update()

    writer = vtk.vtkPNGWriter()
    writer.SetFileName(output)
    writer.SetInputConnection(w2if.GetOutputPort())
    writer.Write()

render('blah', 'test.png')