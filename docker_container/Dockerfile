FROM ubuntu:16.04

ENV FSLDIR=/usr/share/fsl/5.0
ENV FSL_DIR=$FSLDIR
ENV FSLOUTPUTTYPE=NIFTI_GZ
ENV FSLMULTIFILEQUIT=TRUE
ENV FSLTCLSH=${FSLDIR}/bin/fsltclsh
ENV FSLWISH=${FSLDIR}/bin/fslwish
ENV FSLGECUDAQ="cuda.q"
ENV FSL_BIN=${FSLDIR}/bin
ENV FS_OVERRIDE=0
ENV COMPILE_GPU=1
ENV FREESURFER_HOME=/opt/freesurfer
ENV LOCAL_DIR=${FREESURFER_HOME}/local
ENV PERL5LIB=${FREESURFER_HOME}/mni/share/perl5
ENV FSFAST_HOME=${FREESURFER_HOME}/fsfast
ENV FMRI_ANALYSIS_DIR=${FREESURFER_HOME}/fsfast
ENV FSF_OUTPUT_FORMAT="nii.gz"
ENV MNI_DIR=${FREESURFER_HOME}/mni
ENV MNI_DATAPATH=${FREESURFER_HOME}/mni/data
ENV MNI_PERL5LIB=${FREESURFER_HOME}/mni/share/perl5
ENV MINC_BIN_DIR=${FREESURFER_HOME}/mni/bin
ENV MINC_LIB_DIR=${FREESURFER_HOME}/mni/lib
ENV SUBJECTS_DIR=/share
ENV FUNCTIONALS_DIR=${FREESURFER_HOME}/sessions
ENV VTK_DIR=/opt/vtk
ENV MESA_DIR=/opt/mesa
ENV PYTHONPATH=$VTK_DIR/bin:$VTK_DIR/Wrapping/Python``
ENV CUDA_5_LIB_DIR=/usr/local/cuda-5.0/lib64
ENV CUDA_8_LIB_DIR=/usr/local/cuda-8.0/lib64
ENV PATH="${FREESURFER_HOME}/bin:${MNI_DIR}/bin:${FSLDIR}/bin:$PATH"
ENV LD_LIBRARY_PATH="${VTK_DIR}/bin:$MESA_DIR/lib:${FSLDIR}/lib:$LD_LIBRARY_PATH"
ENV OS=LINUX

# Links
RUN MAIN_DIR=$PWD
ARG MESA=https://mesa.freedesktop.org/archive/mesa-18.3.2.tar.gz
ENV VTK=https://gitlab.kitware.com/vtk/vtk/-/archive/v8.2.0.rc2/vtk-v8.2.0.rc2.tar.gz
ENV CUDA5=http://developer.download.nvidia.com/compute/cuda/5_0/rel-update-1/installers/cuda_5.0.35_linux_64_ubuntu11.10-1.run
ENV CUDA8=https://developer.nvidia.com/compute/cuda/8.0/Prod2/local_installers/cuda_8.0.61_375.26_linux-run
ENV FREESURFER=ftp://surfer.nmr.mgh.harvard.edu/pub/dist/freesurfer/6.0.0/freesurfer-Linux-centos6_x86_64-stable-pub-v6.0.0.tar.gz
ENV BEDPOSTX_GPU=http://users.fmrib.ox.ac.uk/~moisesf/Bedpostx_GPU/CUDA_8.0/bedpostx_gpu.zip
ENV PROBTRACKX_GPU=http://users.fmrib.ox.ac.uk/~moisesf/Probtrackx_GPU/CUDA_8.0/probtrackx2_gpu.zip
RUN mkdir /share
RUN apt-get -y update
RUN apt-get install -y apt-transport-https ca-certificates
RUN apt-get install -y software-properties-common
RUN add-apt-repository -y universe
RUN sed -Ei 's/^# deb-src /deb-src /' /etc/apt/sources.list
RUN apt-get -y update
RUN apt-get -y install wget tcsh build-essential curl libtool unzip kmod initramfs-tools locales vim-tiny dkms mricron debhelper dh-autoreconf
RUN locale-gen en_US.UTF-8
RUN rm /bin/sh
RUN ["ln", "-s", "/bin/bash", "/bin/sh"]

# FSL
RUN export DEBIAN_FRONTEND=noninteractive
RUN apt-get install -y tzdata
RUN ln -fs /usr/share/zoneinfo/America/Los_Angeles /etc/localtime
RUN dpkg-reconfigure --frontend noninteractive tzdata
RUN gpg --keyserver hkp://keyserver.ubuntu.com:80 --recv 9BDB3D89CE49EC21
RUN wget -O- http://neuro.debian.net/lists/xenial.us-nh.full | tee /etc/apt/sources.list.d/neurodebian.sources.list
RUN apt-key adv --recv-keys --keyserver hkp://pool.sks-keyservers.net:80 0xA5D32F012649A5A9
RUN apt-get -y update
RUN apt-get -y install fsl-5.0-complete

# VTK
### LLVM
RUN wget --no-check-certificate -O - https://apt.llvm.org/llvm-snapshot.gpg.key | apt-key add -
RUN apt-add-repository "deb http://apt.llvm.org/xenial/ llvm-toolchain-xenial-8 main"
RUN apt-get -y -o Acquire::AllowInsecureRepositories=true update
RUN apt-get -y install libllvm8 llvm-8 llvm-8-dev llvm-8-runtime

### Mesa3D
RUN apt-get -y install libxrandr-dev cmake
RUN apt-get -y build-dep mesa
RUN apt-get -y install freeglut3 freeglut3-dev
RUN wget --no-check-certificate $MESA; \
    MESA_ZIP=$(basename $MESA); \
    MESA_DIR=/opt/mesa; \
    tar -xzf $MESA_ZIP; \
    cd mesa-*; \
    ./configure                                       \
    --prefix=$MESA_DIR                                \
    --enable-opengl --disable-gles1 --disable-gles2   \
    --disable-va --disable-xvmc --disable-vdpau       \
    --enable-shared-glapi                             \
    --disable-texture-float                           \
    --enable-gallium-llvm --enable-llvm-shared-libs   \
    --with-gallium-drivers=swrast                     \
    --disable-dri --with-dri-drivers=                 \
    --disable-egl --with-egl-platforms= --disable-gbm \
    --disable-glx                                     \
    --disable-osmesa --enable-gallium-osmesa          \
    ac_cv_path_LLVM_CONFIG=llvm-config-8; \
    make -j12; \
    make install; \
    rm -rf $MESA_ZIP
RUN cd $MAIN_DIR

### VTK
RUN apt-get -y install python3.5 python3.5-dev python3-pip
RUN VTK_ZIP=$(basename $VTK); \
    wget --no-check-certificate $VTK; \
    tar -xzf $VTK_ZIP; \
    VTK_SRC=$PWD/"${VTK_ZIP%.tar.gz}"; \
    mkdir /opt/vtk; \
    cd /opt/vtk; \
    cmake \
    -DVTK_OPENGL_HAS_OSMESA=ON \
    -DVTK_DEFAULT_RENDER_WINDOW_OFFSCREEN=ON \
    -DVTK_WRAP_PYTHON=ON \
    -DVTK_PYTHON_VERSION=3 \
    -DPYTHON_EXECUTABLE=/usr/bin/python3 \
    -DPYTHON_INCLUDE_PATH=/usr/include/python3.5 \
    -DPYTHON_LIBRARY=/usr/lib/x86_64-linux-gnu/libpython3.5m.so \
    -DOSMESA_INCLUDE_DIR=$MESA_DIR/include \
    -DOSMESA_LIBRARY=$MESA_DIR/lib/libOSMesa.so \
    -DVTK_USE_X=OFF \
    -DBUILD_SHARED_LIBS=ON \
    -DBUILD_TESTING=OFF \
    $VTK_SRC; \
    make -j12; \
    rm -rf $VTK_ZIP
RUN cd $MAIN_DIR
RUN python3.5 -m pip install nibabel pyevtk numpy==1.18.5

# CUDA 5.0
RUN CUDA5_RUN=$(basename $CUDA5); \
    wget --no-check-certificate $CUDA5; \
    sh $CUDA5_RUN -silent -override -toolkit; \
    rm -rf $CUDA5_RUN

# CUDA 8.0
RUN CUDA8_RUN=$(basename $CUDA8); \
    wget --no-check-certificate $CUDA8; \
    sh $CUDA8_RUN -silent -override -toolkit; \
    rm -rf $CUDA8_RUN

# Bedpostx GPU
RUN BEDPOSTX_GPU_ZIP=$(basename $BEDPOSTX_GPU); \
    wget --no-check-certificate $BEDPOSTX_GPU; \
    unzip -o -d /usr/share/fsl/5.0 $BEDPOSTX_GPU_ZIP; \
    cp /usr/share/fsl/5.0/bin/*.so /usr/share/fsl/5.0/lib/; \
    rm -rf $BEDPOSTX_GPU_ZIP

# Probtrackx GPU
RUN PROBTRACKX_GPU_ZIP=$(basename $PROBTRACKX_GPU); \
    wget --no-check-certificate $PROBTRACKX_GPU; \
    unzip -o -d /usr/share/fsl/5.0/bin $PROBTRACKX_GPU_ZIP; \
    rm -rf $PROBTRACKX_GPU_ZIP

# Freesurfer
RUN FREESURFER_GZ=$(basename $FREESURFER); \
    wget --no-check-certificate $FREESURFER; \
    tar -xzf $FREESURFER_GZ -C /opt; \
    rm -rf $FREESURFER_GZ
ADD license.txt /opt/freesurfer/
RUN chmod a+r /opt/freesurfer/license.txt

# Cleanup
RUN apt-get -y clean
