import streamlit as st
import pandas as pd
import joblib
from datetime import datetime


# Configurar la aplicación Streamlit
st.title('Predicción de Demanda e Impacto de Eventos')

# Cargar datos de zonas
zona_data = pd.read_csv('Zonas_Manhathan.csv')  # Ajusta la ruta según la ubicación real del archivo

# Cargar modelos
modelo_demanda = joblib.load('modelo_demanda.pkl')  # Ajusta la ruta según la ubicación real del archivo
modelo_eventos = joblib.load('modelo_eventos.pkl')  # Ajusta la ruta según la ubicación real del archivo

# Cargar datos de eventos
eventos_data = pd.read_csv('Eventosfinal.csv', parse_dates=['Fecha_evento'])
# Ajusta la ruta según la ubicación real del archivo
 # Convertir la fecha a un objeto datetime

# Obtener entrada del usuario (fecha y zona)
fecha = st.date_input('Seleccione la fecha:')
ID_Zona = st.number_input('Ingrese el ID de la Zona:', min_value=1, max_value=300, value=1, step=1)

# Verificar si el ID de Zona pertenece a Manhattan
if ID_Zona not in zona_data['ID_Zona'].values:
    st.warning('ID incorrecto para Manhattan. Por favor, ingrese un ID válido.')
else:
       
    # Crear DataFrame con las características para predecir la demanda
    data_demanda = pd.DataFrame({'Año': [fecha.year], 'Mes': [fecha.month], 'Día': [fecha.day], 'ID Zona': [ID_Zona]})

    # Realizar predicción de demanda
    try:
        prediccion_demanda = modelo_demanda.predict(data_demanda)

        # Mostrar resultado de la predicción de demanda
        st.write(f'Demanda promedio de la zona es de: {round(float(prediccion_demanda[0]), 2)}')

        # Usar el modelo de impacto de eventos para verificar si hay eventos planeados
        evento_planeado = modelo_eventos.predict(data_demanda)

        # Mostrar resultado de la predicción de impacto de eventos
        if not evento_planeado.size > 0:
            st.write('Es probable que la demanda aumente por evento en la zona.')
        else:
            st.write('Demanda promedio sin cambios en la zona.')

    except Exception as e:
        st.error(f'Ocurrió un error durante la predicción: {str(e)}')
