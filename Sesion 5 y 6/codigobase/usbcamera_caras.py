import cv2
import argparse
from cameradrive import streamingmultiplescamaras as cam

window_title = "Camara USB para deteccion de rostros"

"""parametros para los colores de los textos"""
color_texto_ojos = (0, 36, 255)
color_texo_caras = (139, 0, 0)
color_texto_sonrisa = (42, 42, 128)

"""parametro utilizado para la deteccion de rostros"""
color_de_la_cara = (212, 255, 127)
caras_cascade = cv2.CascadeClassifier("haarcascade_frontalface_default.xml")

"""parametros de configuracion para la deteccion de ojos"""
color_de_los_ojos = (255, 212, 0)
ojos_cascade = cv2.CascadeClassifier("haarcascade_eye.xml")

"""parametros de configuracion para la deteccion de sonrisas"""
color_sonrisa = (0, 100, 0)
sonrisa_cascade = cv2.CascadeClassifier("haarcascade_smile.xml")


def reder_frame(frame):
    """
    Procedimiento para mostar el frame la ventana de visualizacion

    Parametros:
    frame: frame del sensor
    """

    bReturn = False

    if cv2.getWindowProperty(window_title, cv2.WND_PROP_AUTOSIZE) >= 0:
        cv2.imshow(window_title, frame)
        bReturn = True
    else:
        bReturn = False

    return bReturn


def drawRect(data, color, frame):
    """
    Funcion para pintar el rectangulo por cada clasificacion:

    Parametros:
    data: datos resultado de la clasificacion
    color: color con el que se va a pintar el rectangulo
    frame: frame leido del sensor
    """

    for x, y, w, h in data:
        cv2.rectangle(frame, (x, y + 5), (x + w + 10, y + h + 20), color, 2)


def drawRectWithConfidence(
    data,
    color_rectangulo,
    frame,
    confidence=None,
    valor_filtro_confidence=0.0,
    tamano_fuente=0.9,
    grosor_fuente=1,
    color_texto=(0, 0, 0),
):
    """
    Descripcion:
    Funcion para pintar el rectangulo por cada clasificacion y adicionalmente imprime en pantalla el valor del porcentaje de efectividad en
    en la clasificacion

    Parametros:
    data: datos resultado de la clasificacion
    color: color con el que se va a pintar el rectangulo
    frame: frame leido del sensor
    confidence: valor del porcentaje de efectividad en la clasificacion
    valor_filtro_confidence: valor para realizar el filtro de la clasificacion
    tamano_fuente: valor float que me indica el tamaño de la fuente a mostrar
    grosor_fuente: valor enteo que me indica el grosor de la fuente a mostar
    color_texto: color con el que se va a imprimir el texto del confidence
    """

    drawRect(data, color_rectangulo, frame)

    if confidence is not None:
        for i, (x, y, w, h) in enumerate(data):
            if confidence[i] >= valor_filtro_confidence:
                confidence_text = f"Conf: {confidence[i]:.2f}"
                cv2.putText(
                    frame,
                    confidence_text,
                    (x, y - 30),
                    cv2.FONT_HERSHEY_SIMPLEX,
                    tamano_fuente,
                    color_texto,
                    grosor_fuente,
                )


def main(
    fps,
    display_width,
    display_height,
    debug,
    enforce_fps,
    gray_scale,
    windows,
    indexcamera,
):
    # Create the Camera instance
    camera = cam(
        fps=fps,
        camera_type=3,
        device_id=indexcamera,
        to_capture_width=display_width,
        to_capture_height=display_height,
        debug=debug,
        enforce_fps=enforce_fps,
        windows=windows,
    )

    if camera.isReady():
        print("USB Camera is now ready")
        cv2.namedWindow(window_title, cv2.WINDOW_AUTOSIZE)

        while True:
            try:
                # read the camera image
                frame = camera.read()

                gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)

                # Convert the frame to grayscale using OpenCV
                if gray_scale:
                    frametransformado = gray
                else:
                    frametransformado = frame

                # Clasificacion de las caras
                caras, caras_rejected, caras_confidence = (
                    caras_cascade.detectMultiScale3(
                        gray,
                        scaleFactor=1.3,
                        minNeighbors=10,
                        outputRejectLevels=True,
                        flags=0,
                    )
                )

                for x, y, w, h in caras:

                    drawRectWithConfidence(
                        caras,
                        color_de_la_cara,
                        frametransformado,
                        caras_confidence,
                        3.5,
                        1.0,
                        2,
                        color_texo_caras,
                    )

                    # Asignacion de las areas de interes a analizar
                    roi_gray = gray[y : y + h, x : x + w]
                    roi_color = frametransformado[y : y + h, x : x + w]

                    # Clasificacion de los ojos
                    ojos, ojos_rejected, ojos_confidence = (
                        ojos_cascade.detectMultiScale3(
                            roi_gray,
                            scaleFactor=1.2,
                            minNeighbors=20,
                            flags=0,
                            outputRejectLevels=True,
                        )
                    )
                    drawRectWithConfidence(
                        ojos,
                        color_de_los_ojos,
                        roi_color,
                        ojos_confidence,
                        1.5,
                        0.6,
                        1,
                        color_texto_ojos,
                    )

                    sonrisa, sonrisa_rejected, sonrisa_confidence = (
                        sonrisa_cascade.detectMultiScale3(
                            roi_gray,
                            scaleFactor=1.8,
                            minNeighbors=35,
                            flags=0,
                            outputRejectLevels=True,
                        )
                    )
                    drawRectWithConfidence(
                        sonrisa,
                        color_sonrisa,
                        roi_color,
                        sonrisa_confidence,
                        2.0,
                        0.7,
                        1,
                        color_texto_sonrisa,
                    )
                    drawRectWithConfidence()

                if not reder_frame(frametransformado):
                    break

                # This also acts as
                keyCodeESC = cv2.waitKey(30) & 0xFF
                keyCodeq = cv2.waitKey(25) & 0xFF

                # Stop the program on the ESC key
                if (keyCodeESC == 27) or (keyCodeq == ord("q")):
                    break

            except KeyboardInterrupt:
                break
    else:
        print("Error opening device: ", indexcamera)

    # close the camera instance
    camera.release()

    # remove camera object
    del camera

    cv2.destroyAllWindows()


if __name__ == "__main__":

    # Initialize the argument parser
    parser = argparse.ArgumentParser(description="Arguments to setup the Camera Object")

    # Define command-line arguments
    parser.add_argument(
        "-f", "--fps", type=int, default=30, help="Frames per seconds", required=False
    )
    parser.add_argument(
        "-w",
        "--windows",
        choices=["true", "false"],
        help="Operating System",
        required=True,
    )
    parser.add_argument(
        "-ic",
        "--indexcamera",
        choices=["0", "1", "2", "3"],
        help="Cameras index",
        required=True,
    )
    parser.add_argument(
        "-dw",
        "--displaywidth",
        type=int,
        default=1280,
        help="Values for the width window",
        required=False,
    )
    parser.add_argument(
        "-dh",
        "--displayheight",
        type=int,
        default=720,
        help="Values for the height window",
        required=False,
    )
    parser.add_argument(
        "--debug", dest="debug", action="store_true", help="To do debug"
    )
    parser.add_argument(
        "--no-debug", dest="debug", action="store_false", help="To do debug"
    )
    parser.add_argument(
        "--gray",
        dest="gray_scale",
        action="store_true",
        help="Convert video to gray scale",
    )
    parser.add_argument(
        "--no-gray",
        dest="gray_scale",
        action="store_false",
        help="Convert video to gray scale",
    )
    parser.add_argument(
        "--enforce",
        dest="enforce_fps",
        action="store_true",
        help="Convert video to gray scale",
    )
    parser.add_argument(
        "--no-enforce",
        dest="enforce_fps",
        action="store_false",
        help="Convert video to gray scale",
    )

    # Parse the arguments
    args = parser.parse_args()

    if args.debug:
        print("Gray Scale in __Main__: ", args.gray_scale)
        print("Debug in __Main__: ", args.debug)
        print("Operating System in __Main__: ", args.windows)
        print("Camera Index in __Main__: ", args.indexcamera)

    main(
        args.fps,
        args.displaywidth,
        args.displayheight,
        args.debug,
        args.enforce_fps,
        args.gray_scale,
        args.windows,
        args.indexcamera,
    )
