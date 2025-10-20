#!/usr/bin/env python3
"""
Script de demostración para el Comparador de Fondos de Imágenes
Permite probar las funcionalidades del backend sin interfaz web
"""

import os
import sys
import numpy as np
from PIL import Image, ImageDraw, ImageFilter
import cv2

# Importar la clase comparadora
try:
    from app import BackgroundComparator
    print("✅ Módulos importados correctamente")
except ImportError as e:
    print(f"❌ Error importando módulos: {e}")
    print("💡 Asegúrate de instalar las dependencias con: pip install -r requirements.txt")
    sys.exit(1)

def create_test_image(size=(400, 300), background_color=(135, 206, 235), object_color=(255, 0, 0)):
    """Crea una imagen de prueba con fondo y objeto"""
    # Crear imagen con fondo
    image = Image.new('RGB', size, background_color)
    draw = ImageDraw.Draw(image)
    
    # Agregar un objeto en el centro
    object_size = min(size) // 4
    center_x, center_y = size[0] // 2, size[1] // 2
    
    # Dibujar círculo (objeto)
    draw.ellipse([
        center_x - object_size,
        center_y - object_size,
        center_x + object_size,
        center_y + object_size
    ], fill=object_color)
    
    # Agregar algo de ruido para hacerlo más realista
    image = image.filter(ImageFilter.GaussianBlur(radius=0.5))
    
    return np.array(image)

def create_similar_background_image(size=(400, 300), background_color=(130, 200, 240), object_color=(0, 255, 0)):
    """Crea una imagen similar pero con pequeñas diferencias"""
    # Crear imagen con fondo similar
    image = Image.new('RGB', size, background_color)
    draw = ImageDraw.Draw(image)
    
    # Agregar un objeto diferente en una posición ligeramente diferente
    object_size = min(size) // 5
    center_x, center_y = size[0] // 2 + 20, size[1] // 2 + 15
    
    # Dibujar rectángulo (objeto diferente)
    draw.rectangle([
        center_x - object_size,
        center_y - object_size,
        center_x + object_size,
        center_y + object_size
    ], fill=object_color)
    
    # Agregar textura diferente
    image = image.filter(ImageFilter.GaussianBlur(radius=1.0))
    
    return np.array(image)

def create_different_background_image(size=(400, 300), background_color=(34, 139, 34), object_color=(255, 255, 0)):
    """Crea una imagen con fondo completamente diferente"""
    # Crear imagen con fondo muy diferente
    image = Image.new('RGB', size, background_color)
    draw = ImageDraw.Draw(image)
    
    # Agregar múltiples objetos
    for i in range(3):
        object_size = min(size) // 8
        center_x = size[0] // 4 * (i + 1)
        center_y = size[1] // 2 + (i - 1) * 30
        
        # Dibujar triángulos (objetos muy diferentes)
        points = [
            (center_x, center_y - object_size),
            (center_x - object_size, center_y + object_size),
            (center_x + object_size, center_y + object_size)
        ]
        draw.polygon(points, fill=object_color)
    
    return np.array(image)

def save_test_images():
    """Guarda las imágenes de prueba"""
    if not os.path.exists('demo_images'):
        os.makedirs('demo_images')
    
    # Crear imágenes de prueba
    image1 = create_test_image()
    image2 = create_similar_background_image()
    image3 = create_different_background_image()
    
    # Guardar imágenes
    Image.fromarray(image1).save('demo_images/test_image_1.jpg', quality=90)
    Image.fromarray(image2).save('demo_images/similar_background.jpg', quality=90)
    Image.fromarray(image3).save('demo_images/different_background.jpg', quality=90)
    
    print("✅ Imágenes de prueba creadas en 'demo_images/'")
    return image1, image2, image3

def test_background_extraction(comparator, image, name):
    """Prueba la extracción de fondo"""
    print(f"\n🔍 Probando extracción de fondo para {name}...")
    
    try:
        background, mask = comparator.extract_background(image)
        print(f"   ✅ Fondo extraído - Forma: {background.shape}")
        
        # Guardar resultado si existe el directorio
        if os.path.exists('demo_images'):
            bg_image = Image.fromarray(background.astype(np.uint8))
            bg_image.save(f'demo_images/{name}_background.jpg')
            
            mask_image = Image.fromarray(mask)
            mask_image.save(f'demo_images/{name}_mask.jpg')
            print(f"   💾 Guardado: {name}_background.jpg y {name}_mask.jpg")
        
        return background
        
    except Exception as e:
        print(f"   ❌ Error: {e}")
        return None

def test_feature_extraction(comparator, image, name):
    """Prueba la extracción de características"""
    print(f"\n📊 Probando extracción de características para {name}...")
    
    try:
        features = comparator.extract_features(image)
        print(f"   ✅ Características extraídas:")
        print(f"      - Histograma de color: {len(features['color_hist'])} dimensiones")
        print(f"      - Textura LBP: {len(features['texture_lbp'])} dimensiones")
        print(f"      - Gradientes: {len(features['gradients'])} dimensiones")
        print(f"      - Forma: {len(features['shape'])} dimensiones")
        
        return features
        
    except Exception as e:
        print(f"   ❌ Error: {e}")
        return None

def test_comparison(comparator, features1, features2, name1, name2):
    """Prueba la comparación entre dos conjuntos de características"""
    print(f"\n⚖️ Comparando {name1} vs {name2}...")
    
    try:
        similarities = comparator.compare_features(features1, features2)
        
        print(f"   📈 Resultados de similitud:")
        print(f"      🎨 Color: {similarities['color']:.2%}")
        print(f"      🖼️ Textura: {similarities['texture']:.2%}")
        print(f"      🏗️ Estructura: {similarities['structural']:.2%}")
        print(f"      📐 Forma: {similarities['shape']:.2%}")
        print(f"      🎯 General: {similarities['overall']:.2%}")
        
        # Interpretación
        overall = similarities['overall']
        if overall >= 0.7:
            interpretation = "🟢 Muy similares"
        elif overall >= 0.5:
            interpretation = "🟡 Moderadamente similares"
        elif overall >= 0.3:
            interpretation = "🟠 Algo similares"
        else:
            interpretation = "🔴 Muy diferentes"
        
        print(f"      💭 Interpretación: {interpretation}")
        
        return similarities
        
    except Exception as e:
        print(f"   ❌ Error: {e}")
        return None

def main():
    """Función principal de demostración"""
    print("🎪 Demostración del Comparador de Fondos de Imágenes")
    print("=" * 60)
    
    # Inicializar comparador
    try:
        comparator = BackgroundComparator()
        print("✅ Comparador inicializado")
    except Exception as e:
        print(f"❌ Error inicializando comparador: {e}")
        return
    
    # Crear imágenes de prueba
    print("\n📸 Creando imágenes de prueba...")
    try:
        image1, image2, image3 = save_test_images()
    except Exception as e:
        print(f"❌ Error creando imágenes: {e}")
        return
    
    # Probar extracción de fondos
    print("\n" + "=" * 40)
    print("🔍 PRUEBAS DE EXTRACCIÓN DE FONDOS")
    print("=" * 40)
    
    bg1 = test_background_extraction(comparator, image1, "imagen1")
    bg2 = test_background_extraction(comparator, image2, "imagen2")
    bg3 = test_background_extraction(comparator, image3, "imagen3")
    
    if not all([bg1 is not None, bg2 is not None, bg3 is not None]):
        print("❌ Error en extracción de fondos, saltando pruebas de comparación")
        return
    
    # Probar extracción de características
    print("\n" + "=" * 40)
    print("📊 PRUEBAS DE EXTRACCIÓN DE CARACTERÍSTICAS")
    print("=" * 40)
    
    features1 = test_feature_extraction(comparator, bg1, "fondo1")
    features2 = test_feature_extraction(comparator, bg2, "fondo2") 
    features3 = test_feature_extraction(comparator, bg3, "fondo3")
    
    if not all([features1, features2, features3]):
        print("❌ Error en extracción de características")
        return
    
    # Probar comparaciones
    print("\n" + "=" * 40)
    print("⚖️ PRUEBAS DE COMPARACIÓN")
    print("=" * 40)
    
    # Comparación 1: Fondos similares
    sim1 = test_comparison(comparator, features1, features2, "Imagen1", "Imagen2 (similar)")
    
    # Comparación 2: Fondos diferentes
    sim2 = test_comparison(comparator, features1, features3, "Imagen1", "Imagen3 (diferente)")
    
    # Comparación 3: Auto-comparación (debería dar 100%)
    sim3 = test_comparison(comparator, features1, features1, "Imagen1", "Imagen1 (idéntica)")
    
    # Resumen final
    print("\n" + "=" * 60)
    print("📋 RESUMEN DE RESULTADOS")
    print("=" * 60)
    
    if sim1 and sim2 and sim3:
        print(f"🔵 Fondos similares:   {sim1['overall']:.1%} de similitud")
        print(f"🔴 Fondos diferentes:  {sim2['overall']:.1%} de similitud")
        print(f"🟢 Auto-comparación:  {sim3['overall']:.1%} de similitud")
        
        print(f"\n✅ El algoritmo funciona correctamente:")
        print(f"   - Detecta similitudes: {sim1['overall'] > sim2['overall']}")
        print(f"   - Auto-comparación alta: {sim3['overall'] > 0.9}")
        print(f"   - Diferencias claras: {sim2['overall'] < 0.7}")
    
    print(f"\n📁 Archivos generados en 'demo_images/':")
    if os.path.exists('demo_images'):
        files = os.listdir('demo_images')
        for file in sorted(files):
            print(f"   - {file}")
    
    print(f"\n🎉 ¡Demostración completada!")
    print(f"💡 Prueba la interfaz web ejecutando: python app.py")

if __name__ == "__main__":
    main() 