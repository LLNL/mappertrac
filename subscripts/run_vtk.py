#!/usr/bin/env python3.5
import vtk, sys
import numpy as np
from os.path import join, split, splitext
from vtk.util import numpy_support

def render(input_file, output_file, histogram_bin_count):
    reader = vtk.vtkNIFTIImageReader()
    reader.SetFileName(input_file)
    reader.Update()
    output = reader.GetOutput()

    size = output.GetDimensions()
    center = output.GetCenter()
    spacing = output.GetSpacing()
    center1 = (center[0], center[1], center[2])
    center2 = (center[0], center[1], center[2])
    if size[2] % 2 == 1:
        center1 = (center[0], center[1], center[2] + 0.5*spacing[2])
    if size[0] % 2 == 1:
        center2 = (center[0] + 0.5*spacing[0], center[1], center[2])
    vrange = output.GetScalarRange()

    avg_scale = 0.25 * (vrange[1]-vrange[0])
    avg_map_top = vtk.vtkImageResliceMapper()
    avg_map_top.BorderOn()
    avg_map_top.SliceAtFocalPointOn()
    avg_map_top.SliceFacesCameraOn()
    avg_map_top.SetSlabThickness(max(size))
    avg_map_top.SetInputConnection(reader.GetOutputPort())
    avg_map_side = vtk.vtkImageResliceMapper()
    avg_map_side.BorderOn()
    avg_map_side.SliceAtFocalPointOn()
    avg_map_side.SliceFacesCameraOn()
    avg_map_side.SetSlabThickness(max(size))
    avg_map_side.SetInputConnection(reader.GetOutputPort())

    avg_top = vtk.vtkImageSlice()
    avg_top.SetMapper(avg_map_top)
    avg_top.GetProperty().SetColorWindow(avg_scale)
    avg_top.GetProperty().SetColorLevel(0.5*avg_scale)
    avg_side = vtk.vtkImageSlice()
    avg_side.SetMapper(avg_map_side)
    avg_side.GetProperty().SetColorWindow(avg_scale)
    avg_side.GetProperty().SetColorLevel(0.5*avg_scale)

    slice_scale = vrange[1]-vrange[0]
    slice_map_top = vtk.vtkImageSliceMapper()
    slice_map_top.BorderOn()
    slice_map_top.SliceAtFocalPointOn()
    slice_map_top.SliceFacesCameraOn()
    slice_map_top.SetInputConnection(reader.GetOutputPort())
    slice_map_side = vtk.vtkImageSliceMapper()
    slice_map_side.BorderOn()
    slice_map_side.SliceAtFocalPointOn()
    slice_map_side.SliceFacesCameraOn()
    slice_map_side.SetInputConnection(reader.GetOutputPort())        

    slice_top = vtk.vtkImageSlice()
    slice_top.SetMapper(slice_map_top)
    slice_top.GetProperty().SetColorWindow(slice_scale)
    slice_top.GetProperty().SetColorLevel(0.5*slice_scale)
    slice_side = vtk.vtkImageSlice()
    slice_side.SetMapper(slice_map_side)
    slice_side.GetProperty().SetColorWindow(slice_scale)
    slice_side.GetProperty().SetColorLevel(0.5*slice_scale)

    text1_actor = vtk.vtkTextActor()
    text1_actor.SetInput("min {:.5g}, max {:.5g}".format(vrange[0], vrange[1]))
    text1_actor.SetPosition(0.1 * center1[0], 0.1 * center1[1])
    text1_actor.GetTextProperty().SetFontSize(18)
    text1_actor.GetTextProperty().SetColor(1,1,1)

    text2_actor = vtk.vtkTextActor()
    text2_actor.SetInput("Scaled Average")
    text2_actor.SetPosition(0.1 * center1[0], 1.8 * size[1])
    text2_actor.GetTextProperty().SetFontSize(24)
    text2_actor.GetTextProperty().SetColor(1,1,1)

    text3_actor = vtk.vtkTextActor()
    text3_actor.SetInput("Slice")
    text3_actor.SetPosition(0.1 * center1[0], 1.8 * size[1])
    text3_actor.GetTextProperty().SetFontSize(24)
    text3_actor.GetTextProperty().SetColor(1,1,1)

    ren1 = vtk.vtkRenderer()
    ren2 = vtk.vtkRenderer()
    ren3 = vtk.vtkRenderer()
    ren4 = vtk.vtkRenderer()
    ren1.SetViewport(0,  0,  0.5,0.5)
    ren2.SetViewport(0.5,0,  1.0,0.5)
    ren3.SetViewport(0,  0.5,0.5,1.0)
    ren4.SetViewport(0.5,0.5,1.0,1.0)
    ren1.AddViewProp(slice_top)
    ren2.AddViewProp(slice_side)
    ren3.AddViewProp(avg_top)
    ren4.AddViewProp(avg_side)
    ren1.AddActor(text1_actor)
    ren3.AddActor(text2_actor)
    ren1.AddActor(text3_actor)

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

    cam3 = ren3.GetActiveCamera()
    cam3.ParallelProjectionOn()
    cam3.SetParallelScale(0.5*spacing[1]*size[1])
    cam3.SetFocalPoint(center1[0], center1[1], center1[2])
    cam3.SetPosition(center1[0], center1[1], center1[2] - 100.0)

    cam4 = ren4.GetActiveCamera()
    cam4.ParallelProjectionOn()
    cam4.SetParallelScale(0.5*spacing[1]*size[1])
    cam4.SetFocalPoint(center2[0], center2[1], center2[2])
    cam4.SetPosition(center2[0] + 100.0, center2[1], center2[2])

    ren_win = vtk.vtkRenderWindow()
    ren_win.SetSize(2 * (size[0] + size[2]) // 2 * 2, 4 * size[1] // 2 * 2) # keep size even
    ren_win.AddRenderer(ren1)
    ren_win.AddRenderer(ren2)
    ren_win.AddRenderer(ren3)
    ren_win.AddRenderer(ren4)
    ren_win.Render()

    # screenshot code:
    w2if = vtk.vtkWindowToImageFilter()
    w2if.SetInput(ren_win)
    w2if.Update()

    writer = vtk.vtkPNGWriter()
    writer.SetFileName(output_file)
    writer.SetInputConnection(w2if.GetOutputPort())
    writer.Write()

    # histogram code:
    num_bins = int(histogram_bin_count)
    histogram = vtk.vtkImageHistogram()
    histogram.SetInputConnection(reader.GetOutputPort())
    histogram.AutomaticBinningOn()
    histogram.SetMaximumNumberOfBins(num_bins)
    histogram.Update()
    origin = histogram.GetBinOrigin()
    spacing = histogram.GetBinSpacing()
    path, output_name = split(output_file)
    output_root, ext = splitext(output_name)
    bin_values = np.linspace(0, num_bins * spacing, num=num_bins, endpoint=False)
    hist_output = join(path, output_root + '_hist.txt')
    hist_values = numpy_support.vtk_to_numpy(histogram.GetHistogram())
    if len(hist_values) != len(bin_values):
        raise Exception('Histogram values and bins do not match')
    hist_mat = np.column_stack((bin_values, hist_values))
    np.savetxt(hist_output, hist_mat, fmt='%.9g')

if __name__ == '__main__':
    if len(sys.argv) != 4:
        raise Exception('Script run_vtk.py requires three arguments - input and output paths, and histogram bin count')
    render(sys.argv[1], sys.argv[2], sys.argv[3])
