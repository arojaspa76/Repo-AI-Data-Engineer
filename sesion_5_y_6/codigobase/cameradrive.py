import cv2
from threading import Thread


class streamingmultiplescamaras:
    """Summary of class streamingmultiplescamaras.

    Attributes:
        fps : It is a int value indicating the frame per second. Default = 0
        camera_type: It is a int value. 0 = CSI Camera, 1 = RSTP Camera, 3 = USB Camera. Default = 0
        device_id : It is a int value. This is used to call CSI and USB devices. 0 = CSI, 1 = USB. to see a list of devices, please run ls /dev/video* in a terminal. Default = 0
    """

    # The __init__ method is called when the object is created
    def __init__(
        self,
        fps=0,
        camera_type=0,
        device_id=0,
        location="https://localhost",
        flip_screen=0,
        to_capture_width=1280,
        to_capture_height=720,
        to_display_width=1280,
        to_display_height=720,
        enforce_fps=False,
        debug=False,
        windows=False,
    ):

        # Operating System
        self.windows = windows

        # initialize all variables
        self.fps = fps
        self.camera_type = camera_type
        self.device_id = device_id

        # for streaming camera only
        self.location = location
        self.flip_screen = flip_screen
        self.to_capture_width = to_capture_width
        self.to_capture_height = to_capture_height
        self.to_display_width = to_display_width
        self.to_display_height = to_display_height
        self.enforce_fps = enforce_fps

        # to check errors in the code
        self.debug_mode = debug

        # track error value
        """
        -1 = Unknown error
        0 = No error
        1 = Error: Could not initialize camera.
        2 = Thread Error: Could not read image from camera
        3 = Error: Could not read image from camera
        4 = Error: Could not release camera
        """

        # Need to keep an history of the error values
        self.error_value = [0]

        # created a thread for enforcing FPS camera read and write
        self.cam_thread = None

        # holds the frame data
        self.frame = None

        # tracks if a CAM opened was succesful or not
        self.cam_opened = False

        # create the OpenCV camera inteface
        self.cap = None

        # open the camera interface
        self.__open_camera()

        # enable a threaded read if enforce_fps is active
        if self.enforce_fps:
            self.__start()

    def __gstreamer_pipeline_CSI(self, sensor_id=0):
        return (
            "nvarguscamerasrc sensor-id=%d ! "
            "video/x-raw(memory:NVMM), width=(int)%d, height=(int)%d, framerate=(fraction)%d/1 ! "
            "nvvidconv flip-method=%d ! "
            "video/x-raw, width=(int)%d, height=(int)%d, format=(string)BGRx ! "
            "videoconvert ! "
            "video/x-raw, format=(string)BGR ! appsink"
            % (
                sensor_id,
                self.to_capture_width,
                self.to_capture_height,
                self.fps,
                self.flip_screen,
                self.to_display_width,
                self.to_display_height,
            )
        )

    def __gstreamer_pipeline_rtsp(self, location="rstp://localhost"):
        return (
            "rtspsrc location=%s ! "
            "rtph264depay ! h264parse ! omxh264dec ! "
            "videorate ! videoscale ! "
            "video/x-raw, width=(int)%d, height=(int)%d, framerate=(fraction)%d/1 ! "
            "videoconvert ! "
            "video/x-raw, format=(string)BGR ! appsink"
            % (
                location,
                self.to_capture_width,
                self.to_capture_height,
                self.fps,
            )
        )

    def __gstreamer_pipeline_usb(self, device_name="/dev/video1"):
        return (
            "v4l2src device=%s ! "
            "image/jpeg, "
            "width=(int)%d, height=(int)%d, pixel-aspect-ratio=(fraction)1/1, "
            "framerate=(fraction)%d/1 ! "
            "jpegdec ! videoconvert ! "
            "appsink"
            % (device_name, self.to_capture_width, self.to_capture_height, self.fps)
        )

    def __gstreamer_pipeline_usb_enforce_fps(self, device_name="/dev/video1"):
        return (
            "v4l2src device=%s ! "
            "image/jpeg, "
            "width=(int)%d, height=(int)%d, pixel-aspect-ratio=(fraction)1/1,"
            "framerate=(fraction)%d/1 ! "
            "jpegdec ! videorate ! "
            "video/x-raw, framerate=(fraction)%d/1 ! "
            "videoflip method=horizontal-flip !"
            "videoconvert ! "
            "appsink"
            % (
                device_name,
                self.to_capture_width,
                self.to_capture_height,
                self.fps,
                self.fps,
            )
        )

    # open the camera inteface
    # determine what type of camera to open
    def __open_camera(self):
        if self.camera_type == 0:  # then CSI camera
            self.__open_csi()
        elif self.camera_type == 1:  # rtsp camera
            self.__open_rtsp()
        elif self.camera_type == 2:  # http camera
            self.__open_mjpeg()
        elif self.camera_type == 3:  # USB camera
            self.__open_usb()
        return self

    # Start threads to read device
    def __start(self):
        self.cam_thread = Thread(target=self.__thread_read)
        self.cam_thread.daemon = True
        self.cam_thread.start()
        return self

    # Tracks if camera is ready or not(maybe something went wrong)
    def isReady(self):
        return self.cam_opened

    # check the current state of the error history
    def hasError(self):
        latest_error = self.error_value[-1]
        if latest_error == 0:
            # means no error has occured yet.
            return self.error_value, False
        else:
            return self.error_value, True

    # opens an inteface to the CSI camera
    def __open_csi(self):
        try:
            # initialize the first CSI camera
            if self.debug_mode:
                print("CSI GStreamer: ", self.__gstreamer_pipeline_CSI(self.device_id))

            self.cap = cv2.VideoCapture(
                self.__gstreamer_pipeline_CSI(self.device_id), cv2.CAP_GSTREAMER
            )

            if (
                not self.cap.isOpened()
            ):  # raise an error here, update the error value parameter
                self.error_value.append(1)
                raise RuntimeError()

            self.cam_opened = True

        except RuntimeError:
            self.cam_opened = False
            if self.debug_mode:
                raise RuntimeError("Error: Could not initialize CSI camera.")

        except Exception:  # some unknown error occurred
            self.error_value.append(-1)
            self.cam_opened = False
            if self.debug_mode:
                raise RuntimeError("Unknown Error has occurred")

    # opens an interface to the RTSP location
    def __open_rtsp(self):

        try:
            if self.debug_mode:
                if self.windows:
                    print("rstp windows: ", self.location)
                else:
                    print(
                        "rstp GStreamer: ",
                        self.__gstreamer_pipeline_rtsp(self.location),
                    )

            # starts the rtsp client
            if self.windows:
                self.cap = cv2.VideoCapture(self.location)
            else:
                self.cap = cv2.VideoCapture(
                    self.__gstreamer_pipeline_rtsp(self.location), cv2.CAP_GSTREAMER
                )

            if (
                not self.cap.isOpened()
            ):  # raise an error here, update the error value parameter
                self.error_value.append(1)
                raise RuntimeError()

            self.cam_opened = True

            if self.windows:
                if self.debug_mode:
                    print(
                        "Configurando el tamaño de la pantalla: width "
                        + str(self.cap.get(cv2.CAP_PROP_FRAME_WIDTH))
                        + ", height: "
                        + str(self.cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
                    )

                self.cap.set(cv2.CAP_PROP_FRAME_WIDTH, int(self.to_capture_width))
                self.cap.set(cv2.CAP_PROP_FRAME_HEIGHT, int(self.to_capture_height))
                self.cap.set(cv2.CAP_PROP_BUFFERSIZE, 3)
                self.cap.set(cv2.CAP_PROP_FPS, self.fps)

                if self.debug_mode:
                    print(
                        "Configurando el tamaño de la pantalla: width "
                        + str(self.cap.get(cv2.CAP_PROP_FRAME_WIDTH))
                        + ", height: "
                        + str(self.cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
                    )

        except RuntimeError:
            self.cam_opened = False
            if self.debug_mode:
                raise RuntimeError("Error: Could not initialize RTSP camera.")

        except Exception:  # some unknown error occurred
            self.error_value.append(-1)
            self.cam_opened = False
            if self.debug_mode:
                raise RuntimeError("Unknown Error has occurred")

    # opens an interface to the USB camera
    def __open_usb(self):
        try:

            # initialize the USB camera
            if self.windows:
                self.camera_name = self.device_id
            else:
                self.camera_name = "/dev/video" + str(self.device_id)

            if self.debug_mode:
                print("USB Camera Device: ", self.camera_name)

            # check if enforcement is enabled
            if self.enforce_fps:
                if self.debug_mode:
                    if self.windows:
                        print("USB enforced windows: ", self.camera_name)
                    else:
                        print(
                            "USB GStreamer enforced: ",
                            self.__gstreamer_pipeline_usb_enforce_fps(self.camera_name),
                        )

                if self.windows:
                    self.cap = cv2.VideoCapture(self.camera_name)
                else:
                    self.cap = cv2.VideoCapture(
                        self.__gstreamer_pipeline_usb_enforce_fps(self.camera_name),
                        cv2.CAP_GSTREAMER,
                    )

            else:
                if self.debug_mode:
                    if self.windows:
                        print("USB windows: ", self.camera_name)
                    else:
                        print(
                            "USB GStreamer: ",
                            self.__gstreamer_pipeline_usb(self.camera_name),
                        )

                if self.windows:
                    self.cap = cv2.VideoCapture(int(self.camera_name))
                else:
                    self.cap = cv2.VideoCapture(
                        self.__gstreamer_pipeline_usb(self.camera_name),
                        cv2.CAP_GSTREAMER,
                    )

            if (
                not self.cap.isOpened()
            ):  # raise an error here, update the error value parameter
                self.error_value.append(1)
                raise RuntimeError()

            self.cam_opened = True

            if self.windows:
                if self.debug_mode:
                    print(
                        "Configurando el tamaño de la pantalla: width "
                        + str(self.cap.get(cv2.CAP_PROP_FRAME_WIDTH))
                        + ", height: "
                        + str(self.cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
                    )

                self.cap.set(cv2.CAP_PROP_FRAME_WIDTH, int(self.to_capture_width))
                self.cap.set(cv2.CAP_PROP_FRAME_HEIGHT, int(self.to_capture_height))
                self.cap.set(cv2.CAP_PROP_FPS, self.fps)

                if self.debug_mode:
                    print(
                        "Configurando el tamaño de la pantalla: width "
                        + str(self.cap.get(cv2.CAP_PROP_FRAME_WIDTH))
                        + ", height: "
                        + str(self.cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
                    )

        except RuntimeError:
            self.cam_opened = False
            if self.debug_mode:
                raise RuntimeError("Error: Could not initialize USB camera.")

        except Exception:  # some unknown error occurred
            self.error_value.append(-1)
            self.cam_opened = False
            if self.debug_mode:
                raise RuntimeError("Unknown Error has occurred")

    # uses thread to read
    def __thread_read(self):
        # time.sleep(1)
        while self.cam_opened:
            try:
                self.frame = self.__read()

            except Exception:
                # update the error value parameter
                self.error_value.append(2)
                self.cam_opened = False
                if self.debug_mode:
                    raise RuntimeError("Thread Error: Could not read image from camera")
                break
        # reset the thread object:
        self.cam_thread = None

    def __read(self):
        # reading images
        ret, image = self.cap.read()
        if ret:
            return image
        else:
            # update the error value parameter
            self.error_value.append(3)

    def read(self):
        # read the camera stream
        try:
            # check if debugging is activated
            if self.debug_mode:
                # check the error value
                if self.error_value[-1] != 0:
                    raise RuntimeError(
                        "An error as occurred. Error Value:", self.error_value
                    )
            if self.enforce_fps:
                # if threaded read is enabled, it is possible the thread hasn't run yet
                if self.frame is not None:
                    return self.frame
                else:
                    # we need to wait for the thread to be ready.
                    return self.__read()
            else:
                return self.__read()
        except Exception as ee:
            if self.debug_mode:
                raise RuntimeError(ee.args)

    def release(self):
        # destroy the opencv camera object
        try:
            # update the cam opened variable
            self.cam_opened = False
            # ensure the camera thread stops running
            if self.enforce_fps:
                if self.cam_thread is not None:
                    self.cam_thread.join()
            if self.cap is not None:
                self.cap.release()
            # update the cam opened variable
            self.cam_opened = False
        except RuntimeError:
            # update the error value parameter
            self.error_value.append(4)
            if self.debug_mode:
                raise RuntimeError("Error: Could not release camera")
