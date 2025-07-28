#!/usr/bin/env python3
"""
Comparador de Imágenes Web
Versión optimizada para despliegue en producción
"""

import os
import time
import logging
from flask import Flask, jsonify, request
from flask_cors import CORS
from PIL import Image, ImageStat, ImageChops

# Configurar logging para producción
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)

class FastImageComparator:
    """Comparador rápido de imágenes optimizado para web"""
    
    def load_image(self, file_stream):
        """Carga y normaliza una imagen rápidamente"""
        try:
            image = Image.open(file_stream)
            
            if image.mode != 'RGB':
                image = image.convert('RGB')
            
            # Redimensionar para comparación rápida
            max_size = 600
            image.thumbnail((max_size, max_size), Image.Resampling.LANCZOS)
            
            return image
        except Exception as e:
            logger.error(f"Error cargando imagen: {e}")
            return None
    
    def compare_images_fast(self, image1, image2):
        """Comparación específica de fondos, ignorando personas centrales"""
        try:
            results = {}
            
            # 1. Comparación de BORDES (donde está el fondo)
            results['edge_similarity'] = self._compare_background_edges(image1, image2)
            
            # 2. Comparación de TEXTURAS (ladrillos, superficies)
            results['texture_similarity'] = self._compare_background_textures(image1, image2)
            
            # 3. Comparación de COLORES dominantes del fondo
            results['color_similarity'] = self._compare_background_colors(image1, image2)
            
            # 4. Hash perceptual de áreas NO centrales
            results['background_hash'] = self._compare_background_hash(image1, image2)
            
            # 5. Análisis estructural de elementos fijos (puertas, ventanas)
            results['structural_similarity'] = self._compare_fixed_elements(image1, image2)
            
            # Cálculo especializado para fondos
            overall = self._calculate_background_similarity(results)
            results['overall_similarity'] = overall
            
            # Mantener compatibilidad con frontend
            results['pixel_similarity'] = results['edge_similarity']
            results['hash_similarity'] = results['background_hash']
            results['stats_similarity'] = results['texture_similarity']
            
            return results
            
        except Exception as e:
            logger.error(f"Error comparando fondos: {e}")
            return self._default_results()
    
    def _compare_background_edges(self, img1, img2):
        """Compara los bordes de las imágenes donde está el fondo"""
        try:
            # Redimensionar para análisis
            size = (300, 200)
            img1_resized = img1.resize(size, Image.Resampling.LANCZOS)
            img2_resized = img2.resize(size, Image.Resampling.LANCZOS)
            
            # Extraer bordes (donde normalmente está el fondo)
            w, h = size
            border_size = 50  # Ancho del borde a analizar
            
            # Extraer regiones de borde
            def extract_borders(img):
                borders = []
                # Borde superior
                borders.append(img.crop((0, 0, w, border_size)))
                # Borde inferior  
                borders.append(img.crop((0, h-border_size, w, h)))
                # Borde izquierdo
                borders.append(img.crop((0, 0, border_size, h)))
                # Borde derecho
                borders.append(img.crop((w-border_size, 0, w, h)))
                return borders
            
            borders1 = extract_borders(img1_resized)
            borders2 = extract_borders(img2_resized)
            
            # Comparar cada borde
            total_similarity = 0
            for b1, b2 in zip(borders1, borders2):
                # Comparar histogramas de cada borde
                hist1 = b1.histogram()
                hist2 = b2.histogram()
                
                # Correlación de histogramas
                correlation = 0
                total1 = sum(hist1) or 1
                total2 = sum(hist2) or 1
                
                for h1, h2 in zip(hist1, hist2):
                    correlation += min(h1/total1, h2/total2)
                
                total_similarity += correlation
            
            # Promedio de todos los bordes
            edge_similarity = total_similarity / len(borders1)
            
            # BOOST para lugares con bordes característicos
            if edge_similarity > 0.4:  # Si hay similitud decente en bordes
                edge_similarity = min(1.0, edge_similarity * 1.25)  # +25% boost
            
            return min(1.0, edge_similarity)
            
        except Exception as e:
            logger.error(f"Error en bordes: {e}")
            return 0.0

    def _compare_background_textures(self, img1, img2):
        """Compara texturas del fondo (ladrillos, superficies)"""
        try:
            # Convertir a escala de grises para análisis de textura
            gray1 = img1.convert('L').resize((200, 150), Image.Resampling.LANCZOS)
            gray2 = img2.convert('L').resize((200, 150), Image.Resampling.LANCZOS)
            
            # Aplicar filtro para detectar texturas
            from PIL import ImageFilter
            edge1 = gray1.filter(ImageFilter.FIND_EDGES)
            edge2 = gray2.filter(ImageFilter.FIND_EDGES)
            
            # Comparar patrones de bordes/texturas
            pixels1 = list(edge1.getdata())
            pixels2 = list(edge2.getdata())
            
            # Calcular similitud de patrones (MÁS TOLERANTE para mismo lugar)
            similar_pixels = 0
            tolerance = 50  # Más tolerante para variaciones de iluminación
            
            for p1, p2 in zip(pixels1, pixels2):
                if abs(p1 - p2) < tolerance:
                    similar_pixels += 1
            
            texture_similarity = similar_pixels / len(pixels1)
            
            # BOOST para texturas decentes (ladrillos detectados)
            if texture_similarity > 0.3:
                texture_similarity = min(1.0, texture_similarity * 1.3)  # +30% boost
            
            return texture_similarity
            
        except Exception as e:
            logger.error(f"Error en texturas: {e}")
            return 0.0

    def _compare_background_colors(self, img1, img2):
        """Compara colores dominantes del fondo MEJORADO"""
        try:
            # Redimensionar para análisis
            size = (200, 150)
            img1_small = img1.resize(size, Image.Resampling.LANCZOS)
            img2_small = img2.resize(size, Image.Resampling.LANCZOS)
            
            w, h = size
            # Área central más pequeña para excluir solo personas
            center_w = w // 3  # Solo el tercio central horizontal
            center_h = h // 3  # Solo el tercio central vertical
            
            def extract_background_regions(img):
                """Extrae múltiples regiones de fondo"""
                regions = []
                
                # Región superior (toda)
                regions.append(img.crop((0, 0, w, h//4)))
                
                # Región inferior (toda)  
                regions.append(img.crop((0, 3*h//4, w, h)))
                
                # Regiones laterales (excluyendo centro)
                regions.append(img.crop((0, h//4, w//4, 3*h//4)))  # Izquierda
                regions.append(img.crop((3*w//4, h//4, w, 3*h//4)))  # Derecha
                
                return regions
            
            regions1 = extract_background_regions(img1_small)
            regions2 = extract_background_regions(img2_small)
            
            total_similarity = 0
            
            for reg1, reg2 in zip(regions1, regions2):
                # Obtener histogramas de cada región
                hist1 = reg1.histogram()
                hist2 = reg2.histogram()
                
                # Simplificar histogramas agrupando colores similares
                def simplify_histogram(hist, bins=32):
                    """Agrupa colores similares para mejor comparación"""
                    simplified = [0] * bins
                    group_size = 256 // bins
                    
                    for i, count in enumerate(hist):
                        group = min(i // group_size, bins - 1)
                        simplified[group] += count
                    
                    return simplified
                
                # Simplificar para R, G, B por separado
                simple_hist1 = []
                simple_hist2 = []
                
                # Procesar cada canal (R, G, B)
                for channel in range(3):
                    start = channel * 256
                    end = start + 256
                    channel_hist1 = hist1[start:end]
                    channel_hist2 = hist2[start:end]
                    
                    simple_hist1.extend(simplify_histogram(channel_hist1))
                    simple_hist2.extend(simplify_histogram(channel_hist2))
                
                # Comparar histogramas simplificados
                total1 = sum(simple_hist1) or 1
                total2 = sum(simple_hist2) or 1
                
                region_similarity = 0
                for h1, h2 in zip(simple_hist1, simple_hist2):
                    freq1 = h1 / total1
                    freq2 = h2 / total2
                    region_similarity += min(freq1, freq2)
                
                total_similarity += region_similarity
            
            # Promedio de todas las regiones
            final_similarity = total_similarity / len(regions1)
            
            # Normalizar y aplicar boost para casos obvios
            final_similarity = min(1.0, final_similarity * 1.2)
            
            return final_similarity
            
        except Exception as e:
            logger.error(f"Error en colores de fondo: {e}")
            return 0.0

    def _compare_background_hash(self, img1, img2):
        """Hash perceptual ESTRICTO enfocado SOLO en áreas de fondo"""
        try:
            # Crear máscaras que excluyan MÁS del centro
            size = (20, 20)  # Más resolución para mejor precisión
            gray1 = img1.convert('L').resize(size, Image.Resampling.LANCZOS)
            gray2 = img2.convert('L').resize(size, Image.Resampling.LANCZOS)
            
            def strict_background_hash(img):
                pixels = list(img.getdata())
                w, h = size
                
                # Solo considerar ESQUINAS EXTREMAS (excluir mucho más del centro)
                bg_pixels = []
                corner_size = 6  # Más área central excluida
                
                for y in range(h):
                    for x in range(w):
                        # Solo esquinas y bordes extremos
                        if (x < corner_size or x >= w - corner_size or 
                            y < corner_size or y >= h - corner_size):
                            # Excluir también área intermedia del centro
                            center_x = w // 2
                            center_y = h // 2
                            distance_from_center = abs(x - center_x) + abs(y - center_y)
                            
                            # Solo incluir si está lejos del centro
                            if distance_from_center > 6:
                                bg_pixels.append(pixels[y * w + x])
                
                if len(bg_pixels) < 10:  # Si muy pocos píxeles, hash genérico
                    return "0" * 32
                
                # Hash más corto pero más específico
                avg = sum(bg_pixels) / len(bg_pixels)
                std_dev = (sum((p - avg) ** 2 for p in bg_pixels) / len(bg_pixels)) ** 0.5
                
                # Considerar varianza para fondos más complejos
                hash_bits = []
                for p in bg_pixels:
                    if std_dev > 10:  # Si hay variación significativa
                        hash_bits.append('1' if p > avg + std_dev/2 else '0')
                    else:  # Fondo uniforme
                        hash_bits.append('1' if p > avg else '0')
                
                return ''.join(hash_bits[:32])  # Limitar tamaño
            
            hash1 = strict_background_hash(gray1)
            hash2 = strict_background_hash(gray2)
            
            # Comparar hashes con más estrictez
            if len(hash1) == len(hash2) and len(hash1) > 10:
                matches = sum(h1 == h2 for h1, h2 in zip(hash1, hash2))
                similarity = matches / len(hash1)
                
                # Penalty si la similitud es solo mediana (posible coincidencia)
                if 0.4 < similarity < 0.8:
                    similarity *= 0.7  # Reducir similitudes mediocres
                    
            else:
                similarity = 0.0
            
            return similarity
            
        except Exception as e:
            logger.error(f"Error en hash de fondo: {e}")
            return 0.0

    def _compare_fixed_elements(self, img1, img2):
        """Detecta elementos fijos como puertas, ventanas, estructuras (MEJORADO)"""
        try:
            # Usar detección de bordes para encontrar elementos estructurales
            from PIL import ImageFilter
            
            gray1 = img1.convert('L').resize((150, 100), Image.Resampling.LANCZOS)
            gray2 = img2.convert('L').resize((150, 100), Image.Resampling.LANCZOS)
            
            # Aplicar múltiples filtros para mejor detección
            edges1 = gray1.filter(ImageFilter.FIND_EDGES)
            edges2 = gray2.filter(ImageFilter.FIND_EDGES)
            
            # También detectar contornos más suaves
            contour1 = gray1.filter(ImageFilter.CONTOUR)
            contour2 = gray2.filter(ImageFilter.CONTOUR)
            
            # Combinar detecciones
            combined1 = ImageChops.add(edges1, contour1)
            combined2 = ImageChops.add(edges2, contour2)
            
            # Comparar patrones estructurales con más tolerancia
            diff = ImageChops.difference(combined1, combined2)
            from PIL import ImageStat
            stat = ImageStat.Stat(diff)
            mean_diff = stat.mean[0]
            
            # Convertir a similitud (MÁS TOLERANTE)
            similarity = max(0, 1 - mean_diff / 160)  # Antes era /128, ahora más tolerante
            
            # BOOST para elementos estructurales detectados
            if similarity > 0.4:  # Si hay cierta similitud estructural
                similarity = min(1.0, similarity * 1.4)  # +40% boost para elementos únicos
            
            return similarity
            
        except Exception as e:
            logger.error(f"Error en elementos fijos: {e}")
            return 0.0

    def _calculate_background_similarity(self, results):
        """Calcula similitud ESTRICTA para evitar falsos positivos"""
        try:
            # Pesos rebalanceados (más conservadores)
            weights = {
                'edge_similarity': 0.30,      # Estructura más importante
                'color_similarity': 0.30,     # Colores críticos
                'texture_similarity': 0.25,   # Texturas (menos peso)
                'structural_similarity': 0.10, # Elementos fijos
                'background_hash': 0.05       # Patrones (menos peso)
            }
            
            overall = 0
            for metric, weight in weights.items():
                if metric in results:
                    overall += results[metric] * weight
            
            # PENALTY para fondos genuinamente diferentes
            # Si TODAS las métricas principales son mediocres, es sospechoso
            main_metrics = ['edge_similarity', 'color_similarity', 'texture_similarity']
            low_metrics = sum(1 for metric in main_metrics 
                            if results.get(metric, 0) < 0.6)
            
            if low_metrics >= 2:  # Si 2+ métricas principales son bajas
                overall *= 0.8  # PENALTY del 20%
            
            # BONIFICACIONES MÁS ESTRICTAS (solo para casos CLAROS)
            
            # 1. Bonus SOLO si texturas son realmente altas
            texture_score = results.get('texture_similarity', 0)
            if texture_score > 0.75:  # Más estricto
                overall = min(1.0, overall * 1.15)  # Menos bonus
            
            # 2. Bonus SOLO si múltiples métricas son REALMENTE altas
            high_metrics = sum(1 for metric in weights.keys() 
                             if results.get(metric, 0) > 0.7)  # Umbral más alto
            
            if high_metrics >= 3:
                overall = min(1.0, overall * 1.2)   # +20% solo si 3+ son ALTAS
            elif high_metrics >= 2:
                overall = min(1.0, overall * 1.1)   # +10% si 2+ son ALTAS
            
            # 3. VERIFICACIÓN FINAL: Evitar falsos positivos
            # Si el promedio general es bajo, limitar resultado máximo
            avg_score = sum(results.get(m, 0) for m in weights.keys()) / len(weights)
            
            if avg_score < 0.5:  # Si promedio es bajo
                overall = min(overall, 0.6)  # Máximo 60%
            elif avg_score < 0.6:  # Si promedio es medio
                overall = min(overall, 0.8)  # Máximo 80%
            
            return overall
            
        except Exception as e:
            logger.error(f"Error calculando similitud: {e}")
            return 0.0

    def _compare_pixels_fast(self, img1, img2):
        """Comparación ultra-rápida de píxeles optimizada para imágenes idénticas"""
        try:
            if img1.size == img2.size:
                diff = ImageChops.difference(img1, img2)
                stat = ImageStat.Stat(diff)
                mean_diff = sum(stat.mean) / len(stat.mean)
                
                if mean_diff < 2.0:
                    return 1.0
            
            size = (200, 150)
            img1_small = img1.resize(size, Image.Resampling.LANCZOS)
            img2_small = img2.resize(size, Image.Resampling.LANCZOS)
            
            pixels1 = list(img1_small.getdata())
            pixels2 = list(img2_small.getdata())
            
            identical_pixels = 0
            total_pixels = len(pixels1)
            
            for p1, p2 in zip(pixels1, pixels2):
                if self._color_distance(p1, p2) < 8:
                    identical_pixels += 1
            
            similarity = identical_pixels / total_pixels
            
            if similarity > 0.99:
                similarity = 1.0
            elif similarity > 0.97:
                similarity = min(1.0, similarity * 1.03)
            elif similarity > 0.94:
                similarity = min(1.0, similarity * 1.02)
                
            return similarity
            
        except Exception as e:
            logger.error(f"Error en píxeles: {e}")
            return 0.0
    
    def _compare_hashes_fast(self, img1, img2):
        """Hash perceptual optimizado"""
        try:
            def enhanced_hash(img):
                thumb = img.resize((16, 16), Image.Resampling.LANCZOS).convert('L')
                pixels = list(thumb.getdata())
                avg = sum(pixels) / len(pixels)
                return ''.join('1' if p > avg else '0' for p in pixels)
            
            hash1 = enhanced_hash(img1)
            hash2 = enhanced_hash(img2)
            
            if hash1 == hash2:
                return 1.0
            
            matches = sum(h1 == h2 for h1, h2 in zip(hash1, hash2))
            similarity = matches / len(hash1)
            
            if similarity > 0.98:
                similarity = 1.0
            elif similarity > 0.95:
                similarity = min(1.0, similarity * 1.02)
            
            return similarity
            
        except Exception as e:
            logger.error(f"Error en hash: {e}")
            return 0.0
    
    def _compare_histograms_fast(self, img1, img2):
        """Comparación rápida de histogramas"""
        try:
            small1 = img1.resize((100, 75), Image.Resampling.LANCZOS)
            small2 = img2.resize((100, 75), Image.Resampling.LANCZOS)
            
            hist1 = small1.histogram()
            hist2 = small2.histogram()
            
            correlation = 0
            total1 = sum(hist1) or 1
            total2 = sum(hist2) or 1
            
            for h1, h2 in zip(hist1, hist2):
                correlation += min(h1/total1, h2/total2)
            
            return correlation
            
        except Exception as e:
            logger.error(f"Error en histograma: {e}")
            return 0.0
    
    def _compare_stats_fast(self, img1, img2):
        """Estadísticas rápidas"""
        try:
            stat1 = ImageStat.Stat(img1)
            stat2 = ImageStat.Stat(img2)
            
            mean1 = stat1.mean
            mean2 = stat2.mean
            
            mean_diff = sum(abs(m1 - m2) for m1, m2 in zip(mean1, mean2)) / len(mean1)
            similarity = max(0, 1 - mean_diff / 255)
            
            return similarity
            
        except Exception as e:
            logger.error(f"Error en stats: {e}")
            return 0.0
    
    def _compare_structure_fast(self, img1, img2):
        """Comparación estructural ultra-rápida"""
        try:
            size = (50, 40)
            img1_tiny = img1.resize(size, Image.Resampling.LANCZOS)
            img2_tiny = img2.resize(size, Image.Resampling.LANCZOS)
            
            diff = ImageChops.difference(img1_tiny, img2_tiny)
            stat = ImageStat.Stat(diff)
            mean_diff = sum(stat.mean) / len(stat.mean)
            
            similarity = max(0, 1 - mean_diff / 128)
            return similarity
            
        except Exception as e:
            logger.error(f"Error en estructura: {e}")
            return 0.0
    
    def _color_distance(self, color1, color2):
        """Distancia rápida entre colores"""
        return abs(color1[0] - color2[0]) + abs(color1[1] - color2[1]) + abs(color1[2] - color2[2])
    
    def _default_results(self):
        """Resultados por defecto"""
        return {
            'pixel_similarity': 0.0,
            'color_similarity': 0.0,
            'stats_similarity': 0.0,
            'structural_similarity': 0.0,
            'hash_similarity': 0.0,
            'overall_similarity': 0.0
        }

# Instancia global del comparador
comparator = FastImageComparator()

@app.route('/')
def index():
    """Página principal con interfaz integrada"""
    return """
<!DOCTYPE html>
<html>
<head>
         <title>🏞️ Comparador de Fondos MercadoPago</title>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <style>
        body { 
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; 
            background: linear-gradient(135deg, #00A6D6 0%, #009EE3 100%);
            margin: 0; padding: 20px; color: white; text-align: center; min-height: 100vh;
        }
        .container { max-width: 1000px; margin: 0 auto; }
        .header { 
            background: rgba(0, 166, 214, 0.2); padding: 30px; border-radius: 15px; 
            margin: 20px 0; border: 1px solid rgba(255,255,255,0.2);
            backdrop-filter: blur(10px);
        }
        .title-container { 
            display: flex; align-items: center; justify-content: center; 
            gap: 20px; flex-wrap: wrap; margin-bottom: 15px;
        }
                 .mp-logo { 
             width: 80px; height: 50px; transition: transform 0.3s ease;
             filter: drop-shadow(0 4px 8px rgba(0,0,0,0.3));
         }
         .mp-logo:hover { transform: scale(1.1); }
        .upload-grid { 
            display: grid; grid-template-columns: 1fr auto 1fr; 
            gap: 30px; align-items: center; margin: 30px 0; 
        }
        .upload-area { 
            border: 3px dashed rgba(255,255,255,0.8); padding: 50px 30px; 
            border-radius: 15px; cursor: pointer; transition: all 0.3s; 
            min-height: 250px; display: flex; flex-direction: column; 
            justify-content: center; background: rgba(255,255,255,0.05);
            backdrop-filter: blur(5px);
        }
        .upload-area:hover { 
            background: rgba(255,255,255,0.15); transform: translateY(-5px); 
            box-shadow: 0 10px 25px rgba(0,0,0,0.3);
        }
        .upload-area.has-file { 
            border-color: #4CAF50; background: rgba(76, 175, 80, 0.2); 
        }
        .vs-divider { 
            width: 80px; height: 80px; 
            background: linear-gradient(135deg, #00A6D6, #009EE3);
            border-radius: 50%; display: flex; align-items: center; 
            justify-content: center; font-weight: bold; font-size: 24px; 
            color: white; box-shadow: 0 8px 16px rgba(0,0,0,0.4);
            border: 3px solid rgba(255,255,255,0.3);
        }
        .btn { 
            background: linear-gradient(45deg, #4CAF50, #45a049); 
            color: white; padding: 18px 40px; border: none; 
            border-radius: 50px; cursor: pointer; margin: 30px; 
            font-size: 18px; font-weight: bold; transition: all 0.3s; 
            box-shadow: 0 6px 12px rgba(0,0,0,0.3);
            text-transform: uppercase; letter-spacing: 1px;
        }
        .btn:hover { 
            background: linear-gradient(45deg, #45a049, #4CAF50); 
            transform: translateY(-3px); 
            box-shadow: 0 8px 16px rgba(0,0,0,0.4);
        }
        .btn:disabled { 
            background: #666; cursor: not-allowed; transform: none; 
            box-shadow: none;
        }
        .result { 
            background: rgba(255,255,255,0.15); padding: 40px; 
            border-radius: 20px; margin: 30px 0; backdrop-filter: blur(15px); 
            border: 1px solid rgba(255,255,255,0.3);
            box-shadow: 0 10px 30px rgba(0,0,0,0.3);
        }
        .similarity-score { 
            font-size: 5em; font-weight: bold; margin: 30px 0; 
            text-shadow: 0 4px 8px rgba(0,0,0,0.3);
            background: linear-gradient(45deg, #FFD700, #FFA500);
            -webkit-background-clip: text; -webkit-text-fill-color: transparent;
            background-clip: text;
        }
        .details-grid { 
            display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); 
            gap: 20px; margin: 30px 0; 
        }
        .detail-item { 
            background: rgba(255,255,255,0.15); padding: 20px; 
            border-radius: 15px; border-left: 5px solid #4CAF50;
            backdrop-filter: blur(5px); transition: transform 0.3s;
        }
        .detail-item:hover { transform: translateY(-2px); }
        .detail-label { font-size: 0.95em; opacity: 0.9; margin-bottom: 8px; }
        .detail-value { font-size: 1.4em; font-weight: bold; }
        .image-preview { 
            max-width: 100%; height: 220px; object-fit: cover; 
            border-radius: 12px; margin-top: 15px; 
            box-shadow: 0 4px 12px rgba(0,0,0,0.3);
        }
        .loading { animation: pulse 1.5s infinite; }
        @keyframes pulse { 0%, 100% { opacity: 1; } 50% { opacity: 0.5; } }
        .speed-badge { 
            background: linear-gradient(45deg, #ff6b6b, #ff5252); 
            color: white; padding: 8px 15px; border-radius: 25px; 
            font-size: 0.85em; margin-left: 15px; font-weight: bold;
            box-shadow: 0 3px 6px rgba(0,0,0,0.3);
        }
        .conclusion { 
            margin-top: 25px; font-size: 1.2em; padding: 20px; 
            background: rgba(255,255,255,0.1); border-radius: 12px;
            border: 1px solid rgba(255,255,255,0.2);
        }
                 @media (max-width: 768px) { 
             .upload-grid { grid-template-columns: 1fr; gap: 20px; }
             .vs-divider { width: 60px; height: 60px; font-size: 18px; }
             .similarity-score { font-size: 3.5em; }
             .title-container { flex-direction: column; gap: 15px; }
             .mp-logo { width: 70px; height: 42px; }
         }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <div class="title-container">
                                 <svg class="mp-logo" viewBox="0 0 200 120" xmlns="http://www.w3.org/2000/svg">
                     <defs>
                         <linearGradient id="mpBg" x1="0%" y1="0%" x2="100%" y2="100%">
                             <stop offset="0%" style="stop-color:#00A6D6;stop-opacity:1" />
                             <stop offset="50%" style="stop-color:#009EE3;stop-opacity:1" />
                             <stop offset="100%" style="stop-color:#0091D4;stop-opacity:1" />
                         </linearGradient>
                         <linearGradient id="handGradient" x1="0%" y1="0%" x2="100%" y2="100%">
                             <stop offset="0%" style="stop-color:#ffffff;stop-opacity:1" />
                             <stop offset="100%" style="stop-color:#f0f8ff;stop-opacity:1" />
                         </linearGradient>
                     </defs>
                     
                     <!-- Fondo oval azul -->
                     <ellipse cx="100" cy="60" rx="95" ry="55" fill="url(#mpBg)" stroke="#003d82" stroke-width="3"/>
                     
                     <!-- Apretón de manos simplificado -->
                     <g transform="translate(100,60)">
                         <!-- Mano izquierda -->
                         <path d="M-35,-15 C-40,-15 -45,-10 -45,-5 L-45,5 C-45,10 -40,15 -35,15 L-5,15 L-5,10 L-10,5 L-15,0 L-20,-5 L-25,-10 L-35,-15 Z" 
                               fill="url(#handGradient)" stroke="#003d82" stroke-width="1.5"/>
                         
                         <!-- Mano derecha -->
                         <path d="M35,-15 C40,-15 45,-10 45,-5 L45,5 C45,10 40,15 35,15 L5,15 L5,10 L10,5 L15,0 L20,-5 L25,-10 L35,-15 Z" 
                               fill="url(#handGradient)" stroke="#003d82" stroke-width="1.5"/>
                         
                         <!-- Dedos entrelazados -->
                         <circle cx="-8" cy="8" r="3" fill="url(#handGradient)" stroke="#003d82" stroke-width="1"/>
                         <circle cx="0" cy="12" r="3" fill="url(#handGradient)" stroke="#003d82" stroke-width="1"/>
                         <circle cx="8" cy="8" r="3" fill="url(#handGradient)" stroke="#003d82" stroke-width="1"/>
                         
                         <!-- Pulgar -->
                         <ellipse cx="-12" cy="-8" rx="4" ry="8" fill="url(#handGradient)" stroke="#003d82" stroke-width="1"/>
                         <ellipse cx="12" cy="-8" rx="4" ry="8" fill="url(#handGradient)" stroke="#003d82" stroke-width="1"/>
                     </g>
                     
                     <!-- Borde inferior decorativo -->
                     <ellipse cx="100" cy="95" rx="80" ry="15" fill="#ffffff" opacity="0.2"/>
                 </svg>
                <h1>🏞️ Comparador de Fondos <span class="speed-badge">⚡ ULTRA RÁPIDO</span></h1>
            </div>
            <p style="font-size: 1.1em; margin: 0;">
                🎯 Detecta si dos fotos tienen el mismo fondo • 👥 Las personas pueden ser diferentes • 🚀 100% preciso
            </p>
        </div>
        
        <div class="upload-grid">
                         <div class="upload-area" id="area1" onclick="document.getElementById('file1').click()">
                 <h3 id="title1">🏞️ Primera Foto</h3>
                 <p>Selecciona la primera imagen para comparar su fondo</p>
                 <input type="file" id="file1" accept="image/*" style="display:none">
                 <img id="preview1" class="image-preview" style="display:none">
             </div>
             
             <div class="vs-divider">VS</div>
             
             <div class="upload-area" id="area2" onclick="document.getElementById('file2').click()">
                 <h3 id="title2">🏞️ Segunda Foto</h3>
                 <p>Selecciona la segunda imagen para comparar su fondo</p>
                 <input type="file" id="file2" accept="image/*" style="display:none">
                 <img id="preview2" class="image-preview" style="display:none">
             </div>
        </div>
        
                 <button class="btn" id="compareBtn" onclick="compareImages()" disabled>🏞️ Comparar Fondos</button>
         
         <div id="result" style="display:none;" class="result">
             <h2>📊 Comparación de Fondos</h2>
            <div class="similarity-score" id="overallScore">--%</div>
            <div class="details-grid">
                                 <div class="detail-item">
                     <div class="detail-label">🏞️ Estructura del Fondo</div>
                     <div class="detail-value" id="pixelSim">--%</div>
                 </div>
                 <div class="detail-item">
                     <div class="detail-label">🎨 Colores del Fondo</div>
                     <div class="detail-value" id="colorSim">--%</div>
                 </div>
                 <div class="detail-item">
                     <div class="detail-label">🔢 Patrones Visuales</div>
                     <div class="detail-value" id="hashSim">--%</div>
                 </div>
                <div class="detail-item">
                    <div class="detail-label">⏱️ Tiempo de Proceso</div>
                    <div class="detail-value" id="timeValue">--s</div>
                </div>
            </div>
            <div id="conclusion" class="conclusion"></div>
        </div>
    </div>
    
    <script>
        let file1 = null, file2 = null;
        
        function setupFileInput(fileInputId, areaId, titleId, previewId) {
            const fileInput = document.getElementById(fileInputId);
            const area = document.getElementById(areaId);
            const title = document.getElementById(titleId);
            const preview = document.getElementById(previewId);
            
            fileInput.addEventListener('change', (e) => {
                const file = e.target.files[0];
                if (file) {
                    if (fileInputId === 'file1') file1 = file;
                    if (fileInputId === 'file2') file2 = file;
                    
                    title.innerHTML = '✅ ' + file.name;
                    area.classList.add('has-file');
                    
                    const reader = new FileReader();
                    reader.onload = (e) => {
                        preview.src = e.target.result;
                        preview.style.display = 'block';
                    };
                    reader.readAsDataURL(file);
                    
                    updateCompareButton();
                }
            });
        }
        
        setupFileInput('file1', 'area1', 'title1', 'preview1');
        setupFileInput('file2', 'area2', 'title2', 'preview2');
        
        function updateCompareButton() {
            const btn = document.getElementById('compareBtn');
            btn.disabled = !(file1 && file2);
        }
        
        function compareImages() {
            if (!file1 || !file2) return;
            
            const startTime = performance.now();
            
            const formData = new FormData();
            formData.append('image1', file1);
            formData.append('image2', file2);
            
            document.getElementById('result').style.display = 'block';
            document.getElementById('overallScore').innerHTML = '⚡';
            document.getElementById('overallScore').classList.add('loading');
            
            fetch('/api/compare-images', {
                method: 'POST',
                body: formData
            })
            .then(response => response.json())
            .then(data => {
                const endTime = performance.now();
                const processingTime = ((endTime - startTime) / 1000).toFixed(2);
                
                document.getElementById('overallScore').classList.remove('loading');
                
                const overall = Math.round(data.overall_similarity * 100);
                document.getElementById('overallScore').innerHTML = overall + '%';
                
                document.getElementById('pixelSim').innerHTML = Math.round(data.pixel_similarity * 100) + '%';
                document.getElementById('colorSim').innerHTML = Math.round(data.color_similarity * 100) + '%';
                document.getElementById('hashSim').innerHTML = Math.round(data.hash_similarity * 100) + '%';
                document.getElementById('timeValue').innerHTML = processingTime + 's';
                
                                 let conclusion = '';
                 if (overall >= 98) {
                     conclusion = '🎯 <strong>¡Mismo fondo detectado!</strong><br>100% seguro que tienen el mismo fondo';
                 } else if (overall >= 90) {
                     conclusion = '✅ <strong>Fondos prácticamente idénticos</strong><br>Muy alta probabilidad de ser el mismo lugar';
                 } else if (overall >= 75) {
                     conclusion = '🟢 <strong>Fondos muy similares</strong><br>Mismo lugar o ubicación, con ligeras diferencias';
                 } else if (overall >= 50) {
                     conclusion = '🟡 <strong>Fondos parcialmente similares</strong><br>Algunos elementos del fondo coinciden';
                 } else {
                     conclusion = '🔴 <strong>Fondos diferentes</strong><br>Las fotos fueron tomadas en lugares distintos';
                 }
                
                document.getElementById('conclusion').innerHTML = conclusion;
            })
            .catch(error => {
                document.getElementById('overallScore').innerHTML = '❌';
                document.getElementById('conclusion').innerHTML = '❌ Error procesando imágenes';
                console.error('Error:', error);
            });
        }
    </script>
</body>
</html>
    """

@app.route('/api/compare-images', methods=['POST'])
@app.route('/api/compare-backgrounds', methods=['POST'])
def compare_images():
    """API de comparación de imágenes"""
    try:
        start_time = time.time()
        
        if 'image1' not in request.files or 'image2' not in request.files:
            return jsonify({'error': 'Se requieren dos imágenes'}), 400
        
        file1 = request.files['image1']
        file2 = request.files['image2']
        
        logger.info(f"Comparando: {file1.filename} vs {file2.filename}")
        
        # Cargar imágenes
        img1 = comparator.load_image(file1.stream)
        img2 = comparator.load_image(file2.stream)
        
        if not img1 or not img2:
            return jsonify({'error': 'Error cargando imágenes'}), 400
        
        # Comparación rápida
        results = comparator.compare_images_fast(img1, img2)
        
        processing_time = round(time.time() - start_time, 3)
        
        logger.info(f"Resultado: {results['overall_similarity']:.2%} en {processing_time}s")
        
        # Respuesta optimizada
        response = {
            'overall_similarity': float(results['overall_similarity']),
            'pixel_similarity': float(results['pixel_similarity']),
            'color_similarity': float(results['color_similarity']),
            'texture_similarity': float(results['stats_similarity']),
            'structural_similarity': float(results['structural_similarity']),
            'hash_similarity': float(results['hash_similarity']),
            'stats_similarity': float(results['stats_similarity']),
            'processing_time': processing_time,
            'message': f'Análisis completado en {processing_time}s'
        }
        
        return jsonify(response)
        
    except Exception as e:
        logger.error(f"Error: {e}")
        return jsonify({'error': f'Error procesando imágenes: {str(e)}'}), 500

@app.route('/api/health')
def health():
    """Verificación de salud del servicio"""
    return jsonify({
        'status': 'ok', 
        'version': 'web_1.0',
        'message': 'Comparador de Imágenes Web - Funcionando correctamente'
    })

# Para producción con Gunicorn
if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    debug_mode = os.environ.get('FLASK_ENV') != 'production'
    
    logger.info("🌐 Iniciando Comparador de Imágenes Web")
    logger.info(f"🚀 Puerto: {port}")
    
    app.run(debug=debug_mode, host='0.0.0.0', port=port) 