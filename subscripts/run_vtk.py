#!/usr/bin/env python2.7
import vtk, sys

def render(input_file, output_file):
    reader = vtk.vtkNIFTIImageReader()
    reader.SetFileName(input_file)
    reader.Update()

    size = reader.GetOutput().GetDimensions()
    center = reader.GetOutput().GetCenter()
    spacing = reader.GetOutput().GetSpacing()
    center1 = (center[0], center[1], center[2])
    center2 = (center[0], center[1], center[2])
    if size[2] % 2 == 1:
        center1 = (center[0], center[1], center[2] + 0.5*spacing[2])
    if size[0] % 2 == 1:
        center2 = (center[0] + 0.5*spacing[0], center[1], center[2])
    vrange = reader.GetOutput().GetScalarRange()

    map1 = vtk.vtkImageSliceMapper()
    map1.BorderOn()
    map1.SliceAtFocalPointOn()
    map1.SliceFacesCameraOn()
    map1.SetInputConnection(reader.GetOutputPort())
    map2 = vtk.vtkImageSliceMapper()
    map2.BorderOn()
    map2.SliceAtFocalPointOn()
    map2.SliceFacesCameraOn()
    map2.SetInputConnection(reader.GetOutputPort())

    slice1 = vtk.vtkImageSlice()
    slice1.SetMapper(map1)
    slice1.GetProperty().SetColorWindow(vrange[1]-vrange[0])
    slice1.GetProperty().SetColorLevel(0.5*(vrange[0]+vrange[1]))
    slice2 = vtk.vtkImageSlice()
    slice2.SetMapper(map2)
    slice2.GetProperty().SetColorWindow(vrange[1]-vrange[0])
    slice2.GetProperty().SetColorLevel(0.5*(vrange[0]+vrange[1]))

    ratio = size[0]*1.0/(size[0]+size[2])

    ren1 = vtk.vtkRenderer()
    ren1.SetViewport(0,0,ratio,1.0)
    ren2 = vtk.vtkRenderer()
    ren2.SetViewport(ratio,0.0,1.0,1.0)
    ren1.AddViewProp(slice1)
    ren2.AddViewProp(slice2)

    cam1 = ren1.GetActiveCamera()
    cam1.ParallelProjectionOn()
    cam1.SetParallelScale(0.5*spacing[1]*size[1])
    cam1.SetFocalPoint(center1[0], center1[1], center1[2])
    cam1.SetPosition(center1[0], center1[1], center1[2] - 100.0)

    cam2 = ren2.GetActiveCamera()
    cam2.ParallelProjectionOn()
    cam2.SetParallelScale(0.5*spacing[1]*size[1])
    cam2.SetFocalPoint(center2[0], center2[1], center2[2])
    cam2.SetPosition(center2[0] + 100.0, center2[1], center2[2])

    renWin = vtk.vtkRenderWindow()
    renWin.SetSize((size[0] + size[2]) // 2 * 2, size[1] // 2 * 2) # keep size even
    renWin.AddRenderer(ren1)
    renWin.AddRenderer(ren2)
    renWin.Render()

    # screenshot code:
    w2if = vtk.vtkWindowToImageFilter()
    w2if.SetInput(renWin)
    w2if.Update()

    writer = vtk.vtkPNGWriter()
    writer.SetFileName(output_file)
    writer.SetInputConnection(w2if.GetOutputPort())
    writer.Write()

if __name__ == '__main__':
    if len(sys.argv) != 3:
        raise Exception('Script run_vtk.py2 requires two arguments - input and output paths')
    render(sys.argv[1], sys.argv[2])