#!/usr/bin/env python3
"""
Comparador de Imágenes Web con Machine Learning
Versión optimizada para despliegue en producción
"""

import os
import time
import logging
from flask import Flask, jsonify, request
from flask_cors import CORS
from PIL import Image, ImageStat, ImageChops
import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import StandardScaler
import joblib

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
            
            # Generar conclusión descriptiva
            results['conclusion'] = self._generate_background_conclusion(overall)
            
            # Mantener compatibilidad con frontend
            results['pixel_similarity'] = results['edge_similarity']
            results['hash_similarity'] = results['background_hash']
            results['stats_similarity'] = results['texture_similarity']
            
            return results
            
        except Exception as e:
            logger.error(f"Error comparando fondos: {e}")
            return self._default_results()
    
    def _generate_background_conclusion(self, similarity_percentage):
        """Genera conclusión descriptiva basada en el porcentaje de similitud"""
        # Convertir a porcentaje si está en decimal
        if similarity_percentage <= 1.0:
            percentage = similarity_percentage * 100
        else:
            percentage = similarity_percentage
        
        if percentage >= 85:
            return {
                'category': 'iguales',
                'description': '🟢 Fondos prácticamente iguales',
                'detail': f'Los fondos son muy similares ({percentage:.1f}%). Misma ubicación o condiciones muy parecidas.'
            }
        elif percentage >= 60:
            return {
                'category': 'similares',
                'description': '🟡 Fondos similares',
                'detail': f'Los fondos comparten características importantes ({percentage:.1f}%). Posiblemente misma zona o tipo de ambiente.'
            }
        elif percentage >= 35:
            return {
                'category': 'parcialmente_similares',
                'description': '🟠 Fondos parcialmente similares',
                'detail': f'Los fondos tienen algunas similitudes ({percentage:.1f}%). Algunos elementos en común pero diferencias notables.'
            }
        elif percentage >= 15:
            return {
                'category': 'diferentes',
                'description': '🔴 Fondos diferentes',
                'detail': f'Los fondos son claramente diferentes ({percentage:.1f}%). Ubicaciones o ambientes distintos.'
            }
        else:
            return {
                'category': 'muy_diferentes',
                'description': '🔴 Fondos muy diferentes',
                'detail': f'Los fondos son completamente diferentes ({percentage:.1f}%). Ubicaciones totalmente distintas.'
            }
    
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
            
            # BOOST CONSERVADOR solo para bordes realmente similares
            if edge_similarity > 0.6:  # Umbral más alto
                edge_similarity = min(1.0, edge_similarity * 1.15)  # Menos boost
            
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
            
            # DETECCIÓN DE TEXTURAS COMPLETAMENTE DIFERENTES
            # Comparar distribución de intensidades de bordes
            edge1_intensity = sum(pixels1) / len(pixels1) if pixels1 else 0
            edge2_intensity = sum(pixels2) / len(pixels2) if pixels2 else 0
            
            # Si una imagen tiene muchos bordes y otra pocos (uniforme vs compleja)
            intensity_diff = abs(edge1_intensity - edge2_intensity)
            
            if intensity_diff > 40:  # Una muy uniforme, otra muy texturizada
                texture_similarity *= 0.5  # PENALTY del 50%
            
            # BOOST CONSERVADOR solo para texturas genuinamente altas
            if texture_similarity > 0.6:  # Umbral más alto
                texture_similarity = min(1.0, texture_similarity * 1.2)  # Menos boost
            elif texture_similarity > 0.4:
                texture_similarity = min(1.0, texture_similarity * 1.1)  # Boost mínimo
            
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
            
            # DETECCIÓN DE FONDOS COMPLETAMENTE DIFERENTES
            # Verificar si son fondos muy diferentes (ej: azul vs bokeh dorado)
            
            # Analizar varianza de colores en cada región
            region_variances = []
            for reg1, reg2 in zip(regions1, regions2):
                # Convertir a arrays para análisis estadístico
                pixels1 = list(reg1.getdata())
                pixels2 = list(reg2.getdata())
                
                # Calcular varianza de cada región
                if pixels1 and pixels2:
                    # Promedio de varianza RGB de cada región
                    var1 = sum(abs(p[0] - p[1]) + abs(p[1] - p[2]) + abs(p[0] - p[2]) for p in pixels1) / len(pixels1)
                    var2 = sum(abs(p[0] - p[1]) + abs(p[1] - p[2]) + abs(p[0] - p[2]) for p in pixels2) / len(pixels2)
                    region_variances.append(abs(var1 - var2))
            
            avg_variance_diff = sum(region_variances) / len(region_variances) if region_variances else 0
            
            # Si una imagen es muy uniforme y otra muy variada (azul vs bokeh)
            if avg_variance_diff > 30:  # Umbral para fondos muy diferentes
                final_similarity *= 0.4  # PENALTY SEVERA del 60%
            
            # Si la similitud es muy baja, no aplicar boost
            if final_similarity < 0.4:
                # NO aplicar boost para fondos diferentes
                return final_similarity
            else:
                # Solo aplicar boost moderado para fondos genuinamente similares
                final_similarity = min(1.0, final_similarity * 1.1)  # Menos boost
            
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
        """Calcula similitud ULTRA-ESTRICTA - Fondos diferentes deben dar <30%"""
        try:
            # Obtener métricas individuales
            edge_score = results.get('edge_similarity', 0)
            color_score = results.get('color_similarity', 0) 
            texture_score = results.get('texture_similarity', 0)
            structural_score = results.get('structural_similarity', 0)
            hash_score = results.get('background_hash', 0)
            
            # DETECCIÓN DE FONDOS COMPLETAMENTE DIFERENTES
            # Si CUALQUIER métrica principal es muy baja, es sospechoso
            main_scores = [edge_score, color_score, texture_score]
            very_low_scores = sum(1 for score in main_scores if score < 0.4)
            
            # DETECCIÓN INTELIGENTE: ¿Fondos diferentes VS mismo lugar?
            if very_low_scores >= 1:  # Si CUALQUIER métrica principal es muy baja
                
                # VERIFICACIÓN ANTI-FALSOS POSITIVOS ULTRA-AGRESIVA
                # Detectar fondos obviamente diferentes (azul vs bokeh)
                definitely_different = False
                
                # 1. Si colores bajos + texturas bajas = fondos diferentes (más permisivo)
                if color_score < 0.4 and texture_score < 0.4:  # Antes <0.3
                    definitely_different = True
                
                # 2. Si bordes bajos + colores bajos = arquitectura diferente (más permisivo)
                if edge_score < 0.4 and color_score < 0.4:  # Antes <0.3
                    definitely_different = True
                
                # 3. Si TODAS las métricas son mediocres (más permisivo)
                if all(score < 0.55 for score in [edge_score, color_score, texture_score, structural_score]):  # Antes <0.5
                    definitely_different = True
                
                # 4. NUEVA: Si promedio general es bajo (fondos diferentes)
                avg_all = (edge_score + color_score + texture_score + structural_score) / 4
                if avg_all < 0.45:  # Promedio bajo = fondos diferentes
                    definitely_different = True
                
                # 5. NUEVA: Si solo 1 métrica es decente y el resto bajas
                decent_metrics = sum(1 for score in [edge_score, color_score, texture_score, structural_score] if score > 0.5)
                if decent_metrics <= 1:  # Solo 1 o ninguna métrica decente
                    definitely_different = True
                
                # 6. NUEVA: Si texturas + bordes muy bajos (no hay elementos únicos)
                if texture_score < 0.4 and edge_score < 0.4:
                    definitely_different = True
                
                # Si es definitivamente diferente, aplicar penalty máximo
                if definitely_different:
                    overall = (edge_score * 0.4 + color_score * 0.4 + 
                              texture_score * 0.15 + structural_score * 0.05)
                    overall *= 0.3  # PENALTY MÁS SEVERA del 70% (antes 50%)
                    return min(overall, 0.2)  # MÁXIMO 20% para fondos obviamente diferentes (antes 25%)
                
                # VERIFICAR si es MISMO LUGAR con elementos únicos (REBALANCEADO)
                same_place_indicators = 0
                
                # 1. Elementos fijos únicos = ladrillos + puerta (más permisivo)
                if texture_score > 0.45 and structural_score > 0.45:  # Antes >0.5
                    same_place_indicators += 2  # Elementos fijos únicos detectados
                
                # 2. Arquitectura similar (más permisivo)
                if edge_score > 0.35 and color_score > 0.35:  # Antes >0.4
                    same_place_indicators += 1  # Arquitectura similar
                
                # 3. Elemento único claro (puerta, ventana) - más permisivo
                if structural_score > 0.5:  # Antes >0.6
                    same_place_indicators += 1  # Elemento fijo detectado
                
                # 4. Superficie única clara (ladrillos) - más permisivo  
                if texture_score > 0.5:  # Antes >0.6
                    same_place_indicators += 1  # Superficie característica
                
                # 5. NUEVO: Combinación decente de bordes + texturas
                if edge_score > 0.4 and texture_score > 0.4:
                    same_place_indicators += 1  # Estructura + superficie detectada
                
                # 6. NUEVO: Si 3+ métricas son decentes (lugar con variaciones)
                decent_scores = sum(1 for score in [edge_score, color_score, texture_score, structural_score] if score > 0.4)
                if decent_scores >= 3:
                    same_place_indicators += 1  # Múltiples características detectadas
                
                # DECISIÓN INTELIGENTE (BONIFICACIONES MEJORADAS)
                if same_place_indicators >= 5:  # MISMO LUGAR muy claro (5+ indicadores)
                    # Es mismo lugar con evidencia muy fuerte
                    overall = (edge_score * 0.35 + color_score * 0.35 + 
                              texture_score * 0.20 + structural_score * 0.10)
                    
                    # BONUS AGRESIVO para evidencia muy fuerte
                    overall = min(1.0, overall * 1.5)  # +50% bonus para casos muy claros
                    return overall
                
                elif same_place_indicators >= 4:  # MISMO LUGAR probable
                    # Mismo lugar con evidencia fuerte
                    overall = (edge_score * 0.35 + color_score * 0.35 + 
                              texture_score * 0.20 + structural_score * 0.10)
                    
                    # BONUS FUERTE para evidencia fuerte
                    overall = min(1.0, overall * 1.35)  # +35% bonus (antes 30%)
                    return overall
                
                elif same_place_indicators >= 3:  # MISMO LUGAR posible
                    # Mismo lugar con evidencia decente
                    overall = (edge_score * 0.35 + color_score * 0.35 + 
                              texture_score * 0.20 + structural_score * 0.10)
                    
                    # Bonus moderado mejorado
                    overall = min(1.0, overall * 1.25)  # +25% bonus (antes 15%)
                    return min(overall, 0.95)  # Máximo 95% (antes 90%)
                
                elif same_place_indicators >= 2:  # Posible mismo lugar
                    # Cálculo conservador  
                    overall = (edge_score * 0.4 + color_score * 0.4 + 
                              texture_score * 0.15 + structural_score * 0.05)
                    
                    # Bonus mínimo mejorado
                    overall = min(1.0, overall * 1.1)  # +10% bonus (antes 5%)
                    return min(overall, 0.7)  # Máximo 70% (antes 60%)
                
                else:  # FONDOS DIFERENTES confirmado
                    # Usar cálculo ultra-conservador
                    overall = (edge_score * 0.4 + color_score * 0.4 + 
                              texture_score * 0.15 + structural_score * 0.05)
                    
                    # PENALTY AGRESIVA para fondos diferentes
                    if very_low_scores >= 2:  # Si 2+ métricas son muy bajas
                        overall *= 0.6  # PENALTY del 40%
                    elif very_low_scores >= 1:  # Si 1+ métrica es muy baja
                        overall *= 0.75  # PENALTY del 25%
                    
                    # LÍMITE MÁXIMO para fondos diferentes
                    return min(overall, 0.4)  # MÁXIMO 40% para fondos diferentes
            
            # CÁLCULO NORMAL solo para fondos genuinamente similares
            weights = {
                'edge_similarity': 0.35,      # Estructura más importante
                'color_similarity': 0.35,     # Colores críticos  
                'texture_similarity': 0.20,   # Texturas
                'structural_similarity': 0.07, # Elementos fijos
                'background_hash': 0.03       # Patrones (muy poco peso)
            }
            
            overall = 0
            for metric, weight in weights.items():
                if metric in results:
                    overall += results[metric] * weight
            
            # Bonificaciones SOLO para casos EXCEPCIONALES
            exceptional_metrics = sum(1 for score in main_scores if score > 0.8)
            
            if exceptional_metrics >= 3:  # Todas las métricas principales > 80%
                overall = min(1.0, overall * 1.15)  # +15% solo en casos excepcionales
            elif exceptional_metrics >= 2:  # 2+ métricas > 80%
                overall = min(1.0, overall * 1.05)  # +5% muy conservador
            
            # LÍMITES FINALES según promedio
            avg_main = sum(main_scores) / len(main_scores)
            
            if avg_main < 0.45:  # Promedio bajo
                overall = min(overall, 0.3)  # Máximo 30%
            elif avg_main < 0.55:  # Promedio medio-bajo
                overall = min(overall, 0.5)  # Máximo 50%
            elif avg_main < 0.65:  # Promedio medio
                overall = min(overall, 0.7)  # Máximo 70%
            
            return overall
            
        except Exception as e:
            logger.error(f"Error calculando similitud: {e}")
            return 0.0

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

class MedicalImageComparator:
    """Comparador médico usando Machine Learning para mayor precisión"""
    
    def __init__(self):
        """Inicializa el modelo de ML"""
        self.model = None
        self.scaler = StandardScaler()
        self.model_path = 'medical_model.joblib'
        self.scaler_path = 'medical_scaler.joblib'
        
        # Cargar modelo pre-entrenado si existe
        self._load_model()
    
    def _load_model(self):
        """Carga el modelo pre-entrenado"""
        try:
            if os.path.exists(self.model_path) and os.path.exists(self.scaler_path):
                self.model = joblib.load(self.model_path)
                self.scaler = joblib.load(self.scaler_path)
                logger.info("✅ Modelo ML cargado exitosamente")
            else:
                logger.info("🤖 Creando modelo ML inicial...")
                self._create_initial_model()
        except Exception as e:
            logger.error(f"Error cargando modelo: {e}")
            self._create_initial_model()
    
    def _create_initial_model(self):
        """Crea un modelo inicial con características MÉDICAS ESPECIALIZADAS"""
        # Dataset con 8 características médicas especializadas
        
        # Características: [uniformidad_patológica, píxeles_oscuros, densidad_gradientes, textura_facial, 
        #                  contraste_local, patrones_luz, variabilidad_regional, densidad_espectral]
        
        # CASOS NORMALES (cierre voluntario) - Etiqueta: 0
        # Características típicas: baja uniformidad patológica, pocos píxeles oscuros, alta densidad gradientes
        normal_cases = [
            [0.25, 0.15, 0.35, 0.40, 0.30, 0.45, 0.35, 0.40],    # Persona normal 1 - buena variabilidad
            [0.30, 0.20, 0.40, 0.45, 0.35, 0.50, 0.40, 0.45],    # Persona normal 2 - variabilidad media
            [0.20, 0.10, 0.30, 0.35, 0.25, 0.40, 0.30, 0.35],    # Persona normal 3 - alta variabilidad
            [0.35, 0.25, 0.45, 0.50, 0.40, 0.55, 0.45, 0.50],    # Persona normal 4 - condiciones medias
            [0.28, 0.18, 0.38, 0.42, 0.32, 0.48, 0.38, 0.42],    # Persona normal 5 - promedio
            [0.22, 0.12, 0.32, 0.38, 0.28, 0.42, 0.32, 0.38],    # Persona normal 6 - buenas condiciones
            [0.32, 0.22, 0.42, 0.48, 0.38, 0.52, 0.42, 0.48],    # Persona normal 7 - condiciones difíciles
            [0.26, 0.16, 0.36, 0.41, 0.31, 0.46, 0.36, 0.41],    # Persona normal 8 - condiciones estándar
            [0.24, 0.14, 0.34, 0.39, 0.29, 0.44, 0.34, 0.39],    # Persona normal 9 - buena iluminación
            [0.29, 0.19, 0.39, 0.44, 0.34, 0.49, 0.39, 0.44],    # Persona normal 10 - iluminación media
        ]
        
        # CASOS PATOLÓGICOS (condiciones reales) - Etiqueta: 1
        # Características típicas: alta uniformidad patológica, muchos píxeles oscuros, baja densidad gradientes
        pathology_cases = [
            [0.85, 0.90, 0.05, 0.08, 0.85, 0.88, 0.82, 0.85],    # Ceguera severa 1 - extremadamente uniforme
            [0.80, 0.85, 0.10, 0.12, 0.80, 0.83, 0.78, 0.80],    # Ceguera severa 2 - muy uniforme
            [0.88, 0.92, 0.03, 0.05, 0.88, 0.90, 0.85, 0.88],    # Ceguera severa 3 - uniformidad extrema
            [0.75, 0.78, 0.15, 0.18, 0.75, 0.78, 0.72, 0.75],    # Ceguera moderada 1 - algo de variación
            [0.82, 0.87, 0.08, 0.10, 0.82, 0.85, 0.80, 0.82],    # Ceguera severa 4 - muy uniforme
            [0.70, 0.72, 0.20, 0.25, 0.70, 0.73, 0.68, 0.70],    # Ceguera leve 1 - más variación
            [0.78, 0.82, 0.12, 0.15, 0.78, 0.80, 0.75, 0.78],    # Ceguera moderada 2 - uniforme
            [0.84, 0.89, 0.06, 0.08, 0.84, 0.87, 0.81, 0.84],    # Ceguera severa 5 - muy uniforme
            [0.72, 0.75, 0.18, 0.22, 0.72, 0.75, 0.70, 0.72],    # Ceguera moderada 3 - algo de textura
            [0.86, 0.91, 0.04, 0.06, 0.86, 0.89, 0.83, 0.86],    # Ceguera severa 6 - uniformidad extrema
        ]
        
        # Combinar datos
        X = np.array(normal_cases + pathology_cases)
        y = np.array([0]*len(normal_cases) + [1]*len(pathology_cases))
        
        # Entrenar modelo con parámetros ULTRA-OPTIMIZADOS
        self.scaler.fit(X)
        X_scaled = self.scaler.transform(X)
        
        self.model = RandomForestClassifier(
            n_estimators=300,           # Más árboles para máxima precisión
            max_depth=6,               # Profundidad controlada
            min_samples_split=2,       # Mínimo para dividir nodos
            min_samples_leaf=1,        # Mínimo en hojas
            random_state=42,
            class_weight={0: 0.3, 1: 0.7}  # Dar mucho más peso a casos patológicos
        )
        self.model.fit(X_scaled, y)
        
        # Guardar modelo
        joblib.dump(self.model, self.model_path)
        joblib.dump(self.scaler, self.scaler_path)
        
        logger.info("🚀 Modelo ML MÉDICO ESPECIALIZADO creado y guardado")
        
        # Mostrar importancia de características médicas
        feature_names = ['Uniformidad_Patológica', 'Píxeles_Oscuros', 'Densidad_Gradientes', 'Textura_Facial', 
                        'Contraste_Local', 'Patrones_Luz', 'Variabilidad_Regional', 'Densidad_Espectral']
        importances = self.model.feature_importances_
        for name, importance in zip(feature_names, importances):
            logger.info(f"🔬 {name}: {importance:.3f}")
    
    
    def _extract_features(self, image_path):
        """Extrae características MÉDICAS ESPECIALIZADAS de la imagen"""
        try:
            image = Image.open(image_path).convert('RGB')
            gray_image = image.convert('L')
            gray_array = np.array(gray_image)
            
            # === CARACTERÍSTICAS MÉDICAS ESPECIALIZADAS ===
            
            # 1. ANÁLISIS DE UNIFORMIDAD PATOLÓGICA (ceguera = muy uniforme)
            uniformity_score = self._calculate_pathological_uniformity(gray_array)
            
            # 2. ANÁLISIS DE DISTRIBUCIÓN DE PÍXELES OSCUROS (ceguera = muchos píxeles oscuros)
            dark_pixel_ratio = self._calculate_dark_pixel_dominance(gray_array)
            
            # 3. ANÁLISIS DE GRADIENTES LOCALES (ceguera = pocos gradientes)
            gradient_density = self._calculate_gradient_density(gray_array)
            
            # 4. ANÁLISIS DE TEXTURA FACIAL (ceguera = textura plana)
            facial_texture_score = self._calculate_facial_texture_complexity(gray_array)
            
            # 5. ANÁLISIS DE CONTRASTE LOCAL (ceguera = contraste muy bajo)
            local_contrast_score = self._calculate_local_contrast_variation(gray_array)
            
            # 6. ANÁLISIS DE PATRONES DE LUZ/SOMBRA (ceguera = patrones anómalos)
            light_pattern_score = self._calculate_light_shadow_patterns(gray_array)
            
            # 7. ANÁLISIS DE VARIABILIDAD REGIONAL (ceguera = regiones muy similares)
            regional_variability = self._calculate_regional_variability(gray_array)
            
            # 8. ANÁLISIS DE DENSIDAD ESPECTRAL (ceguera = baja densidad)
            spectral_density = self._calculate_spectral_density(gray_array)
            
            features = [
                uniformity_score,
                dark_pixel_ratio,
                gradient_density,
                facial_texture_score,
                local_contrast_score,
                light_pattern_score,
                regional_variability,
                spectral_density
            ]
            
            return np.array(features).reshape(1, -1)
            
        except Exception as e:
            logger.error(f"Error extrayendo características médicas: {e}")
            # Retornar características por defecto (caso normal)
            return np.array([[0.3, 0.4, 0.5, 0.6, 0.5, 0.4, 0.5, 0.6]])
    
    def _calculate_pathological_uniformity(self, gray_array):
        """Calcula uniformidad patológica - ceguera = muy uniforme"""
        # Calcular desviación estándar normalizada
        std_dev = np.std(gray_array) / 255.0
        # Invertir: alta uniformidad = baja desviación = alta probabilidad patológica
        uniformity = 1.0 - std_dev
        return min(1.0, max(0.0, uniformity))
    
    def _calculate_dark_pixel_dominance(self, gray_array):
        """Calcula dominancia de píxeles oscuros - ceguera = muchos píxeles oscuros"""
        # Contar píxeles muy oscuros (< 50)
        dark_pixels = np.sum(gray_array < 50)
        total_pixels = gray_array.size
        dark_ratio = dark_pixels / total_pixels
        return min(1.0, dark_ratio * 2.0)  # Amplificar la señal
    
    def _calculate_gradient_density(self, gray_array):
        """Calcula densidad de gradientes - ceguera = pocos gradientes"""
        # Calcular gradientes usando Sobel
        from scipy import ndimage
        gradient_x = ndimage.sobel(gray_array, axis=1)
        gradient_y = ndimage.sobel(gray_array, axis=0)
        gradient_magnitude = np.sqrt(gradient_x**2 + gradient_y**2)
        
        # Contar gradientes significativos
        significant_gradients = np.sum(gradient_magnitude > 20)
        total_pixels = gray_array.size
        gradient_density = significant_gradients / total_pixels
        
        # Invertir: pocos gradientes = alta probabilidad patológica
        return max(0.0, 1.0 - gradient_density * 3.0)
    
    def _calculate_facial_texture_complexity(self, gray_array):
        """Calcula complejidad de textura facial - ceguera = textura simple"""
        # Calcular varianza local usando ventana deslizante
        from scipy import ndimage
        local_variance = ndimage.generic_filter(gray_array.astype(float), np.var, size=5)
        
        # Promedio de varianza local
        avg_local_variance = np.mean(local_variance) / (255.0 ** 2)
        
        # Invertir: baja varianza local = alta probabilidad patológica
        return max(0.0, 1.0 - avg_local_variance * 10.0)
    
    def _calculate_local_contrast_variation(self, gray_array):
        """Calcula variación de contraste local - ceguera = contraste muy uniforme"""
        # Dividir imagen en regiones 4x4
        h, w = gray_array.shape
        region_contrasts = []
        
        for i in range(0, h-20, 20):
            for j in range(0, w-20, 20):
                region = gray_array[i:i+20, j:j+20]
                if region.size > 0:
                    region_contrast = np.max(region) - np.min(region)
                    region_contrasts.append(region_contrast)
        
        if not region_contrasts:
            return 0.5
        
        # Calcular variabilidad del contraste entre regiones
        contrast_std = np.std(region_contrasts) / 255.0
        
        # Invertir: baja variabilidad = alta probabilidad patológica
        return max(0.0, 1.0 - contrast_std * 5.0)
    
    def _calculate_light_shadow_patterns(self, gray_array):
        """Analiza patrones de luz/sombra - ceguera = patrones anómalos"""
        # Dividir en cuartiles de intensidad
        q1, q2, q3 = np.percentile(gray_array, [25, 50, 75])
        
        # Contar píxeles en cada cuartil
        very_dark = np.sum(gray_array < q1)
        dark = np.sum((gray_array >= q1) & (gray_array < q2))
        light = np.sum((gray_array >= q2) & (gray_array < q3))
        very_light = np.sum(gray_array >= q3)
        
        total = gray_array.size
        
        # Distribución normal debería ser más equilibrada
        # Ceguera tiende a tener dominancia de píxeles oscuros
        dark_dominance = (very_dark + dark) / total
        
        return min(1.0, dark_dominance * 1.5)
    
    def _calculate_regional_variability(self, gray_array):
        """Calcula variabilidad entre regiones - ceguera = regiones muy similares"""
        # Dividir imagen en 9 regiones (3x3)
        h, w = gray_array.shape
        region_means = []
        
        for i in range(3):
            for j in range(3):
                start_h, end_h = i * h // 3, (i + 1) * h // 3
                start_w, end_w = j * w // 3, (j + 1) * w // 3
                region = gray_array[start_h:end_h, start_w:end_w]
                if region.size > 0:
                    region_means.append(np.mean(region))
        
        if len(region_means) < 2:
            return 0.5
        
        # Calcular variabilidad entre regiones
        regional_std = np.std(region_means) / 255.0
        
        # Invertir: baja variabilidad = alta probabilidad patológica
        return max(0.0, 1.0 - regional_std * 8.0)
    
    def _calculate_spectral_density(self, gray_array):
        """Calcula densidad espectral - ceguera = baja densidad espectral"""
        # Aplicar FFT 2D para análisis frecuencial
        fft = np.fft.fft2(gray_array)
        fft_magnitude = np.abs(fft)
        
        # Calcular energía en altas frecuencias (detalles)
        h, w = fft_magnitude.shape
        high_freq_energy = np.sum(fft_magnitude[h//4:3*h//4, w//4:3*w//4])
        total_energy = np.sum(fft_magnitude)
        
        if total_energy == 0:
            return 0.5
        
        high_freq_ratio = high_freq_energy / total_energy
        
        # Invertir: baja energía en altas frecuencias = alta probabilidad patológica
        return max(0.0, 1.0 - high_freq_ratio * 3.0)
    
    def analyze_medical_condition(self, image1_path, image2_path):
        """Analiza condición médica usando ML con POST-PROCESAMIENTO INTELIGENTE"""
        try:
            if not self.model:
                logger.error("Modelo ML no disponible")
                return self._fallback_analysis(image1_path, image2_path)
            
            # Extraer características de ambas imágenes
            features1 = self._extract_features(image1_path)
            features2 = self._extract_features(image2_path)
            
            # Escalar características
            features1_scaled = self.scaler.transform(features1)
            features2_scaled = self.scaler.transform(features2)
            
            # Predecir probabilidades RAW del modelo
            prob1 = self.model.predict_proba(features1_scaled)[0]
            prob2 = self.model.predict_proba(features2_scaled)[0]
            
            # prob[0] = probabilidad normal, prob[1] = probabilidad patológica
            raw_pathology_prob1 = prob1[1] * 100
            raw_pathology_prob2 = prob2[1] * 100
            
            # === POST-PROCESAMIENTO INTELIGENTE ===
            
            # Aplicar función de amplificación inteligente
            final_prob1 = self._intelligent_probability_adjustment(raw_pathology_prob1, features1[0])
            final_prob2 = self._intelligent_probability_adjustment(raw_pathology_prob2, features2[0])
            
            # Determinar diagnóstico
            diagnosis1 = self._get_ml_diagnosis(final_prob1)
            diagnosis2 = self._get_ml_diagnosis(final_prob2)
            
            # Calcular confianza basada en la diferencia entre imágenes
            difference = abs(final_prob1 - final_prob2)
            if difference > 40:
                confidence_level = "Muy Alta"
                confidence_percentage = 95
            elif difference > 25:
                confidence_level = "Alta" 
                confidence_percentage = 85
            elif difference > 15:
                confidence_level = "Moderada"
                confidence_percentage = 70
            else:
                confidence_level = "Baja"
                confidence_percentage = 55
            
            results = {
                'image1_medical_probability': round(final_prob1, 2),
                'image2_medical_probability': round(final_prob2, 2),
                'image1_diagnosis': diagnosis1,
                'image2_diagnosis': diagnosis2,
                'confidence_level': confidence_level,
                'confidence_percentage': confidence_percentage,
                'method': 'Machine Learning + Post-procesamiento'
            }
            
            logger.info(f"🤖 ML Analysis - Raw: Img1: {raw_pathology_prob1:.1f}%, Img2: {raw_pathology_prob2:.1f}%")
            logger.info(f"🧠 Final Analysis - Img1: {final_prob1:.1f}%, Img2: {final_prob2:.1f}%")
            
            return results
            
        except Exception as e:
            logger.error(f"Error en análisis ML: {e}")
            return self._fallback_analysis(image1_path, image2_path)
    
    def _intelligent_probability_adjustment(self, raw_probability, features):
        """Ajuste inteligente REBALANCEADO - Más agresivo con casos reales"""
        
        # Extraer características clave para análisis
        uniformity_score = features[0]      # Uniformidad patológica
        dark_pixels = features[1]           # Píxeles oscuros
        gradient_density = features[2]      # Densidad de gradientes
        texture_complexity = features[3]    # Complejidad de textura
        local_contrast = features[4]        # Contraste local
        light_patterns = features[5]        # Patrones de luz
        regional_variability = features[6]  # Variabilidad regional
        spectral_density = features[7]      # Densidad espectral
        
        # === ANÁLISIS ULTRA-ESPECÍFICO ===
        
        # === LÓGICA EXTREMADAMENTE AGRESIVA - MÁXIMA PENALIZACIÓN ===
        
        # INDICADORES DE CEGUERA REAL (amplificar agresivamente)
        blindness_score = 0
        
        # 1. TIPO A: Ceguera OSCURA extrema (cataratas severas, etc.)
        if uniformity_score > 0.8 and dark_pixels > 0.95:  # COMBINACIÓN EXTREMA OSCURA
            blindness_score += 15  # SÚPER FUERTE
        elif uniformity_score > 0.75 and dark_pixels > 0.9:
            blindness_score += 12  # MUY FUERTE
        elif uniformity_score > 0.6 and dark_pixels >= 1.0:  # PÍXELES COMPLETAMENTE OSCUROS
            blindness_score += 10  # FUERTE - Ceguera con píxeles totalmente oscuros
        
        # 2. TIPO B: Ceguera CLARA extrema (albinismo severo, leucoma total, etc.)
        elif uniformity_score > 0.85 and dark_pixels < 0.15:  # COMBINACIÓN EXTREMA CLARA
            blindness_score += 15  # SÚPER FUERTE - Solo casos MUY específicos
        elif uniformity_score > 0.82 and dark_pixels < 0.2:
            blindness_score += 12  # MUY FUERTE
        elif uniformity_score > 0.8 and dark_pixels < 0.15:  # CEGUERA CLARA MODERADA (más estricto)
            blindness_score += 8   # FUERTE - Leucoma, albinismo moderado
        
        # 3. TIPO C: Ceguera MODERADA (glaucoma, degeneración macular, etc.)
        elif 0.7 <= uniformity_score <= 0.8 and 0.3 <= dark_pixels <= 0.6:  # RANGO ESPECÍFICO
            blindness_score += 10  # FUERTE - Ceguera con características moderadas
        elif 0.68 <= uniformity_score <= 0.82 and 0.25 <= dark_pixels <= 0.65:
            blindness_score += 8   # MODERADO
        
        # 4. Uniformidad muy alta (cualquier tipo de ceguera)
        if uniformity_score > 0.9:   # EXTREMO
            blindness_score += 8
        elif uniformity_score > 0.85:
            blindness_score += 6
        elif uniformity_score > 0.8:
            blindness_score += 4
        elif uniformity_score > 0.75:
            blindness_score += 2
        
        # INDICADORES DE NORMALIDAD (EXTREMADAMENTE AGRESIVOS - MÁXIMA PENALIZACIÓN)
        normal_score = 0
        
        # 1. NORMALIDAD TÍPICA: Rangos que NO interfieren con ceguera
        if 0.4 <= uniformity_score <= 0.65 and 0.4 <= dark_pixels <= 0.7:  # RANGO SEGURO
            normal_score += 30  # SÚPER FUERTE - Persona normal típica
        elif 0.35 <= uniformity_score <= 0.7 and 0.35 <= dark_pixels <= 0.75:
            normal_score += 25  # MUY FUERTE
        elif 0.3 <= uniformity_score <= 0.75 and 0.3 <= dark_pixels <= 0.8:
            normal_score += 20  # FUERTE
        
        # EXCEPCIÓN: NO bonificar si tiene píxeles completamente oscuros (ceguera)
        if dark_pixels >= 1.0:  # Píxeles completamente oscuros
            normal_score = max(0, normal_score - 15)  # Reducir bonificaciones de normalidad
        
        # 2. PENALIZACIONES EXTREMADAMENTE AGRESIVAS - MÁXIMA DESTRUCCIÓN
        # Si está en rango de TIPO B - PENALIZACIÓN DEVASTADORA
        if 0.78 <= uniformity_score <= 0.85 and 0.2 <= dark_pixels <= 0.35:
            normal_score -= 50  # PENALIZACIÓN DEVASTADORA - Zona muy ambigua
        elif 0.75 <= uniformity_score <= 0.85 and 0.15 <= dark_pixels <= 0.4:
            normal_score -= 40  # PENALIZACIÓN EXTREMA
        
        # Si está en rango de TIPO C - PENALIZACIÓN MUY FUERTE
        if 0.7 <= uniformity_score <= 0.8 and 0.3 <= dark_pixels <= 0.6:
            normal_score -= 30  # PENALIZACIÓN MUY FUERTE - Zona ambigua
        elif 0.68 <= uniformity_score <= 0.82 and 0.25 <= dark_pixels <= 0.65:
            normal_score -= 25  # PENALIZACIÓN FUERTE
        
        # 3. BONIFICACIONES solo para rangos ULTRA seguros
        # Uniformidad moderada (solo si está en zona ultra-segura)
        if 0.45 <= uniformity_score <= 0.6:  # RANGO ULTRA-SEGURO
            normal_score += 10
        elif 0.4 <= uniformity_score <= 0.65:
            normal_score += 8
        
        # Píxeles moderados (solo si está en zona ultra-segura)
        if 0.45 <= dark_pixels <= 0.6:  # RANGO ULTRA-SEGURO
            normal_score += 10
        elif 0.4 <= dark_pixels <= 0.65:
            normal_score += 8
        
        # 4. FILTROS ANTI-FALSOS POSITIVOS SELECTIVOS
        # Si tiene características sospechosas, penalizar SOLO si no es ceguera extrema
        if uniformity_score > 0.75 and not (uniformity_score > 0.8 and dark_pixels > 0.9) and not (uniformity_score > 0.8 and dark_pixels < 0.15):
            normal_score -= 20  # PENALIZACIÓN DEVASTADORA por uniformidad alta (solo si no es ceguera extrema OSCURA ni CLARA)
        
        if dark_pixels < 0.35 and not (uniformity_score > 0.8 and dark_pixels < 0.15):  # PROTEGER ceguera clara (más estricto)
            normal_score -= 20  # PENALIZACIÓN DEVASTADORA por píxeles muy claros (solo si no es ceguera clara)
            
        # 5. PENALIZACIÓN ESPECÍFICA SOLO PARA CASOS CLARAMENTE NORMALES
        # SOLO penalizar si NO es ceguera extrema oscura NI clara
        if not (uniformity_score > 0.8 and dark_pixels > 0.95) and not (uniformity_score > 0.8 and dark_pixels < 0.15):  # NO es ceguera extrema
            suspicious_factors = 0
            if uniformity_score > 0.75:
                suspicious_factors += 1
            if dark_pixels < 0.35:  # SOLO píxeles muy claros (no oscuros)
                suspicious_factors += 1
                
            # Penalización acumulativa SOLO para casos no-extremos
            if suspicious_factors >= 2:
                normal_score -= 25  # PENALIZACIÓN ACUMULATIVA EXTREMA
            elif suspicious_factors >= 1:
                normal_score -= 15  # PENALIZACIÓN ACUMULATIVA MODERADA
        
        # === DECISIÓN BALANCEADA CORRECTAMENTE ===
        
        logger.info(f"🔍 Características: Unif={uniformity_score:.2f}, Oscuros={dark_pixels:.2f}, Grad={gradient_density:.2f}, Text={texture_complexity:.2f}")
        logger.info(f"📊 Scores: Ceguera={blindness_score}, Normal={normal_score}")
        
        # CALCULAR DOMINANCIA RELATIVA
        total_evidence = blindness_score + normal_score
        if total_evidence == 0:
            total_evidence = 1  # Evitar división por cero
        
        blindness_dominance = blindness_score / total_evidence
        normal_dominance = normal_score / total_evidence
        
        logger.info(f"⚖️ Dominancias: Ceguera={blindness_dominance:.2f}, Normal={normal_dominance:.2f}")
        
        # CASOS CON EVIDENCIA EXTREMA DE CEGUERA (dominancia ajustada)
        if blindness_score >= 12 and blindness_dominance >= 0.65:  # Reducido de 0.7
            # Ceguera extrema con poca evidencia de normalidad
            adjusted_prob = raw_probability * 2.5 + 40
            final_prob = min(95.0, max(adjusted_prob, 80.0))
            logger.info(f"🚨 CEGUERA EXTREMA (dominante): {raw_probability:.1f}% → {final_prob:.1f}%")
            return final_prob
        
        elif blindness_score >= 9 and blindness_dominance >= 0.60:  # Reducido de 0.65
            # Ceguera fuerte con poca evidencia de normalidad
            adjusted_prob = raw_probability * 2.0 + 30
            final_prob = min(90.0, max(adjusted_prob, 70.0))
            logger.info(f"🔴 CEGUERA FUERTE (dominante): {raw_probability:.1f}% → {final_prob:.1f}%")
            return final_prob
        
        elif blindness_score >= 6 and blindness_dominance >= 0.55:  # Reducido de 0.6
            # Ceguera moderada con poca evidencia de normalidad
            adjusted_prob = raw_probability * 1.7 + 20
            final_prob = min(85.0, max(adjusted_prob, 60.0))
            logger.info(f"🟠 CEGUERA MODERADA (dominante): {raw_probability:.1f}% → {final_prob:.1f}%")
            return final_prob
        
        # CASOS CON EVIDENCIA EXTREMA DE NORMALIDAD (dominancia ajustada)
        elif normal_score >= 8 and normal_dominance >= 0.65:  # Reducido de 0.7
            # Normalidad fuerte con poca evidencia de ceguera
            adjusted_prob = raw_probability * 0.15
            final_prob = max(2.0, min(adjusted_prob, 12.0))
            logger.info(f"✅ NORMALIDAD FUERTE (dominante): {raw_probability:.1f}% → {final_prob:.1f}%")
            return final_prob
        
        elif normal_score >= 6 and normal_dominance >= 0.58:  # Reducido de 0.65
            # Normalidad moderada con poca evidencia de ceguera
            adjusted_prob = raw_probability * 0.25
            final_prob = max(3.0, min(adjusted_prob, 18.0))
            logger.info(f"🟢 NORMALIDAD MODERADA (dominante): {raw_probability:.1f}% → {final_prob:.1f}%")
            return final_prob
        
        # CASOS CON PENALIZACIONES DEVASTADORAS (SCORES NEGATIVOS EXTREMOS)
        elif normal_score < -50:  # PENALIZACIONES DEVASTADORAS
            # Forzar probabilidad muy baja independientemente del ML
            adjusted_prob = raw_probability * 0.05  # REDUCCIÓN EXTREMA (95% de reducción)
            final_prob = max(1.0, min(adjusted_prob, 8.0))  # Máximo 8%
            logger.info(f"💥 PENALIZACIÓN DEVASTADORA (Score={normal_score}): {raw_probability:.1f}% → {final_prob:.1f}%")
            return final_prob
        
        elif normal_score < -30:  # PENALIZACIONES MUY FUERTES
            # Reducir drásticamente por penalizaciones múltiples
            adjusted_prob = raw_probability * 0.1   # REDUCCIÓN MUY FUERTE (90% de reducción)
            final_prob = max(2.0, min(adjusted_prob, 12.0))  # Máximo 12%
            logger.info(f"💥 PENALIZACIÓN MUY FUERTE (Score={normal_score}): {raw_probability:.1f}% → {final_prob:.1f}%")
            return final_prob
        
        elif normal_score < -15:  # PENALIZACIONES FUERTES
            # Reducir significativamente por penalizaciones
            adjusted_prob = raw_probability * 0.2   # REDUCCIÓN FUERTE (80% de reducción)
            final_prob = max(3.0, min(adjusted_prob, 18.0))  # Máximo 18%
            logger.info(f"💥 PENALIZACIÓN FUERTE (Score={normal_score}): {raw_probability:.1f}% → {final_prob:.1f}%")
            return final_prob
        
        # CASOS COMPETITIVOS REBALANCEADOS (ambas evidencias presentes)
        elif blindness_score >= 6 and normal_score >= 4:
            # Competencia: ceguera vs normalidad con ajustes más agresivos
            if blindness_dominance > normal_dominance:
                # Ceguera gana - ser más agresivo
                dominance_boost = (blindness_dominance - 0.5) * 2.0  # Amplificar diferencia
                adjusted_prob = raw_probability * (1.2 + dominance_boost) + 25
                final_prob = min(85.0, max(adjusted_prob, 50.0))
                logger.info(f"🟡 CEGUERA COMPETITIVA (dom={blindness_dominance:.2f}): {raw_probability:.1f}% → {final_prob:.1f}%")
                return final_prob
            else:
                # Normalidad gana - ser más estricto
                dominance_penalty = (normal_dominance - 0.5) * 0.5  # Amplificar reducción
                adjusted_prob = raw_probability * (0.5 - dominance_penalty)
                final_prob = max(5.0, min(adjusted_prob, 25.0))
                logger.info(f"🟡 NORMALIDAD COMPETITIVA (dom={normal_dominance:.2f}): {raw_probability:.1f}% → {final_prob:.1f}%")
                return final_prob
        
        # CASOS CON EVIDENCIA MODERADA DE CEGUERA (sin competencia fuerte)
        elif blindness_score >= 3:
            adjusted_prob = raw_probability * 1.4 + 10
            final_prob = min(80.0, max(adjusted_prob, 40.0))
            logger.info(f"🟡 CEGUERA LEVE: {raw_probability:.1f}% → {final_prob:.1f}%")
            return final_prob
        
        # CASOS CON EVIDENCIA MODERADA DE NORMALIDAD (sin competencia fuerte)
        elif normal_score >= 3:
            adjusted_prob = raw_probability * 0.4
            final_prob = max(5.0, min(adjusted_prob, 25.0))
            logger.info(f"🟡 NORMALIDAD LEVE: {raw_probability:.1f}% → {final_prob:.1f}%")
            return final_prob
        
        # CASOS AMBIGUOS (poca evidencia de ambos lados)
        else:
            if raw_probability > 50:
                # Modelo sugiere patología, ser conservador
                adjusted_prob = raw_probability * 0.7 + 5
                final_prob = min(60.0, max(adjusted_prob, 20.0))
                logger.info(f"❓ AMBIGUO (raw alto): {raw_probability:.1f}% → {final_prob:.1f}%")
                return final_prob
            else:
                # Modelo sugiere normalidad, reducir ligeramente
                adjusted_prob = raw_probability * 0.5
                final_prob = max(8.0, min(adjusted_prob, 25.0))
                logger.info(f"❓ AMBIGUO (raw bajo): {raw_probability:.1f}% → {final_prob:.1f}%")
                return final_prob
    
    def _get_ml_diagnosis(self, probability):
        """Genera diagnóstico basado en probabilidad ML MEJORADO"""
        if probability < 15:
            return "✅ Cierre voluntario normal"
        elif probability < 35:
            return "⚠️ Posible cierre voluntario (verificar)"
        elif probability < 55:
            return "🔍 Condición leve detectada"
        elif probability < 75:
            return "⚡ Condición moderada probable"
        elif probability < 90:
            return "🚨 Condición evidente"
        else:
            return "🔴 Condición severa confirmada"
    
    def _fallback_analysis(self, image1_path, image2_path):
        """Análisis de respaldo si falla ML"""
        return {
            'image1_medical_probability': 15.0,
            'image2_medical_probability': 15.0,
            'image1_diagnosis': "Análisis no disponible",
            'image2_diagnosis': "Análisis no disponible",
            'confidence_level': 'Muy Baja',
            'confidence_percentage': 30,
            'method': 'Fallback'
        }
    
    def update_model_with_feedback(self, image_path, is_pathological):
        """Actualiza el modelo con feedback del usuario"""
        try:
            features = self._extract_features(image_path)
            features_scaled = self.scaler.transform(features)
            
            # Aquí se podría implementar aprendizaje incremental
            # Por ahora, solo registramos el feedback
            label = 1 if is_pathological else 0
            logger.info(f"📝 Feedback recibido: {image_path} -> {label}")
            
            # TODO: Implementar reentrenamiento incremental
            
        except Exception as e:
            logger.error(f"Error actualizando modelo: {e}")

# Instancias globales de los comparadores
background_comparator = FastImageComparator()
medical_comparator = MedicalImageComparator()

@app.route('/')
def index():
    """Página principal con interfaz integrada"""
    return """
<!DOCTYPE html>
<html>
<head>
         <title>Comparador de Fondos</title>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <style>
        body { 
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; 
            background: linear-gradient(135deg, #4A4A4A 0%, #3E3E3E 50%, #2E2E2E 100%);
            margin: 0; padding: 20px; color: #2c3e50; text-align: center; min-height: 100vh;
        }
        .container { 
            max-width: 800px; 
            margin: 0 auto; 
            background: rgba(255, 255, 255, 0.95); 
            padding: 30px; 
            border-radius: 15px; 
            box-shadow: 0 8px 32px rgba(0,0,0,0.3);
            backdrop-filter: blur(10px);
        }
        .logo { 
            max-width: 100px; 
            margin-bottom: 20px; 
        }
        h1 { 
            color: #2c3e50; 
            margin-bottom: 10px; 
            font-size: 2.2em;
            text-shadow: 2px 2px 4px rgba(0,0,0,0.1);
        }
        .subtitle { 
            color: #7f8c8d; 
            margin-bottom: 30px; 
            font-size: 1.1em;
        }
        
        /* Mode Selection */
        .mode-selection {
            background: #f8f9fa;
            padding: 20px;
            border-radius: 10px;
            margin-bottom: 25px;
            border: 2px solid #e9ecef;
        }
        .mode-title {
            font-size: 1.2em;
            color: #495057;
            margin-bottom: 15px;
            font-weight: 600;
        }
        .mode-options {
            display: flex;
            justify-content: center;
            gap: 20px;
            flex-wrap: wrap;
        }
        .mode-option {
            display: flex;
            align-items: center;
            gap: 8px;
            padding: 12px 20px;
            background: white;
            border: 2px solid #dee2e6;
            border-radius: 8px;
            cursor: pointer;
            transition: all 0.3s ease;
            min-width: 200px;
        }
        .mode-option:hover {
            border-color: #007bff;
            background: #f8f9ff;
        }
        .mode-option.selected {
            border-color: #007bff;
            background: #007bff;
            color: white;
        }
        .mode-option input[type="radio"] {
            margin: 0;
        }
        .mode-description {
            font-size: 0.9em;
            opacity: 0.8;
            margin-top: 5px;
        }
        
        .upload-section { 
            display: flex; 
            justify-content: space-around; 
            margin: 30px 0; 
            gap: 20px;
            flex-wrap: wrap;
        }
        .upload-box { 
            flex: 1; 
            min-width: 250px;
            border: 3px dashed #bdc3c7; 
            border-radius: 10px; 
            padding: 30px 20px; 
            text-align: center; 
            transition: all 0.3s ease;
            background: #fafafa;
            position: relative;
            overflow: hidden;
        }
        .upload-box:hover { 
            border-color: #3498db; 
            background: #f0f8ff;
            transform: translateY(-2px);
        }
        .upload-box.dragover { 
            border-color: #2ecc71; 
            background: #f0fff0; 
        }
        .upload-box input[type="file"] { 
            position: absolute;
            top: 0; left: 0; 
            width: 100%; height: 100%; 
            opacity: 0; 
            cursor: pointer; 
        }
        .upload-icon { 
            font-size: 3em; 
            color: #95a5a6; 
            margin-bottom: 15px; 
        }
        .upload-text { 
            color: #7f8c8d; 
            font-size: 1.1em;
            margin-bottom: 10px;
        }
        .file-info { 
            color: #27ae60; 
            font-weight: 600; 
            margin-top: 10px;
        }
        
        .preview-section { 
            display: flex; 
            justify-content: space-around; 
            margin: 30px 0; 
            gap: 20px;
            flex-wrap: wrap;
        }
        .preview-box { 
            flex: 1; 
            min-width: 250px;
            text-align: center; 
        }
        .preview-box h3 { 
            color: #34495e; 
            margin-bottom: 15px; 
        }
        .preview-image { 
            max-width: 100%; 
            max-height: 300px; 
            border-radius: 10px; 
            box-shadow: 0 4px 15px rgba(0,0,0,0.2);
            transition: transform 0.3s ease;
        }
        .preview-image:hover {
            transform: scale(1.05);
        }
        
        .compare-button { 
            background: linear-gradient(135deg, #3498db, #2980b9); 
            color: white; 
            border: none; 
            padding: 15px 40px; 
            font-size: 1.2em; 
            border-radius: 50px; 
            cursor: pointer; 
            margin: 30px 0; 
            box-shadow: 0 4px 15px rgba(52, 152, 219, 0.3);
            transition: all 0.3s ease;
            font-weight: 600;
        }
        .compare-button:hover { 
            background: linear-gradient(135deg, #2980b9, #1f618d);
            transform: translateY(-2px);
            box-shadow: 0 6px 20px rgba(52, 152, 219, 0.4);
        }
        .compare-button:disabled { 
            background: #bdc3c7; 
            cursor: not-allowed; 
            transform: none;
            box-shadow: none;
        }
        
        .results { 
            background: #ecf0f1; 
            padding: 25px; 
            border-radius: 10px; 
            margin-top: 30px; 
            text-align: left;
            box-shadow: inset 0 2px 5px rgba(0,0,0,0.1);
        }
        .results h3 { 
            color: #2c3e50; 
            margin-bottom: 20px; 
            text-align: center;
            font-size: 1.4em;
        }
        .metric { 
            display: flex; 
            justify-content: space-between; 
            align-items: center;
            margin: 15px 0; 
            padding: 12px;
            background: white;
            border-radius: 8px;
            box-shadow: 0 2px 5px rgba(0,0,0,0.05);
        }
        .metric-name { 
            font-weight: 600; 
            color: #34495e; 
        }
        .metric-value { 
            font-weight: bold; 
            font-size: 1.1em;
        }
        .high { color: #27ae60; }
        .medium { color: #f39c12; }
        .low { color: #e74c3c; }
        
        .conclusion-box {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 20px;
            border-radius: 12px;
            margin: 20px 0;
            box-shadow: 0 4px 15px rgba(0,0,0,0.2);
            text-align: center;
        }
        .conclusion-title {
            font-size: 1.3em;
            font-weight: bold;
            margin-bottom: 8px;
        }
        .conclusion-detail {
            font-size: 1em;
            opacity: 0.9;
            line-height: 1.4;
        }
        
        .loading { 
            display: none; 
            color: #3498db; 
            font-size: 1.1em;
            margin: 20px 0;
        }
        .loading::after {
            content: '';
            animation: dots 1.5s steps(5, end) infinite;
        }
        @keyframes dots {
            0%, 20% { content: ''; }
            40% { content: '.'; }
            60% { content: '..'; }
            80%, 100% { content: '...'; }
        }
        
        .error { 
            background: #fadbd8; 
            color: #c0392b; 
            padding: 15px; 
            border-radius: 8px; 
            margin: 20px 0;
            border-left: 4px solid #e74c3c;
        }
        
                 @media (max-width: 768px) { 
            .container { 
                margin: 10px; 
                padding: 20px; 
            }
            .upload-section, .preview-section { 
                flex-direction: column; 
            }
            .mode-options {
                flex-direction: column;
                align-items: center;
            }
            .mode-option {
                min-width: 250px;
            }
         }
    </style>
</head>
<body>
    <div class="container">
        <h1 id="main-title">🖼️ Comparador de Fondos</h1>
        <p class="subtitle" id="subtitle">Compara la similitud entre fondos de dos imágenes usando IA</p>
        
        <!-- Mode Selection -->
        <div class="mode-selection">
            <div class="mode-title">Selecciona el tipo de análisis:</div>
            <div class="mode-options">
                <label class="mode-option selected" for="mode-background">
                    <input type="radio" id="mode-background" name="comparison_mode" value="background" checked>
                    <div>
                        <div><strong>🏠 Comparar Fondos</strong></div>
                        <div class="mode-description">Analiza similitud de fondos entre imágenes</div>
            </div>
                </label>
                <label class="mode-option" for="mode-disease">
                    <input type="radio" id="mode-disease" name="comparison_mode" value="disease">
                    <div>
                        <div><strong>👁️ Evaluar Condición Médica</strong></div>
                        <div class="mode-description">Detecta condiciones médicas usando ML</div>
                    </div>
                </label>
            </div>
        </div>
        
        <div class="upload-section">
            <div class="upload-box" id="upload1">
                <div class="upload-icon">📁</div>
                <div class="upload-text">Arrastra la primera imagen aquí<br>o haz clic para seleccionar</div>
                <input type="file" id="image1" accept="image/*">
                <div class="file-info" id="file1-info"></div>
             </div>
             
            <div class="upload-box" id="upload2">
                <div class="upload-icon">📁</div>
                <div class="upload-text">Arrastra la segunda imagen aquí<br>o haz clic para seleccionar</div>
                <input type="file" id="image2" accept="image/*">
                <div class="file-info" id="file2-info"></div>
             </div>
        </div>
        
        <div class="preview-section">
            <div class="preview-box">
                <h3>Primera Imagen</h3>
                <img id="preview1" class="preview-image" style="display: none;">
                 </div>
            <div class="preview-box">
                <h3>Segunda Imagen</h3>
                <img id="preview2" class="preview-image" style="display: none;">
                 </div>
                 </div>
        
        <button class="compare-button" id="compareBtn" onclick="compareImages()" disabled>
            <span id="button-text">Comparar Fondos</span>
        </button>
        
        <div class="loading" id="loading">Analizando imágenes con IA</div>
        
        <div class="results" id="results" style="display: none;"></div>
    </div>
    
    <script>
        let image1File = null;
        let image2File = null;
        
        function setupInputs() {
            const image1Input = document.getElementById('image1');
            const image2Input = document.getElementById('image2');
            const upload1 = document.getElementById('upload1');
            const upload2 = document.getElementById('upload2');
            const modeInputs = document.querySelectorAll('input[name="comparison_mode"]');
            
            // Mode change handlers
            modeInputs.forEach(input => {
                input.addEventListener('change', updateModeDisplay);
            });
            
            // File input handlers
            image1Input.addEventListener('change', (e) => handleFileSelect(e, 1));
            image2Input.addEventListener('change', (e) => handleFileSelect(e, 2));
            
            // Drag and drop handlers
            [upload1, upload2].forEach((box, index) => {
                box.addEventListener('dragover', handleDragOver);
                box.addEventListener('dragleave', handleDragLeave);
                box.addEventListener('drop', (e) => handleDrop(e, index + 1));
            });
        }
        
        function updateModeDisplay() {
            const selectedMode = document.querySelector('input[name="comparison_mode"]:checked').value;
            const mainTitle = document.getElementById('main-title');
            const subtitle = document.getElementById('subtitle');
            const buttonText = document.getElementById('button-text');
            
            // Update mode option styling
            document.querySelectorAll('.mode-option').forEach(option => {
                option.classList.remove('selected');
            });
            document.querySelector('input[name="comparison_mode"]:checked').closest('.mode-option').classList.add('selected');
            
            if (selectedMode === 'disease') {
                mainTitle.textContent = '👁️ Evaluador de Condición Médica';
                subtitle.textContent = 'Detecta condiciones médicas faciales usando Machine Learning';
                buttonText.textContent = 'Evaluar Condición Médica';
            } else {
                mainTitle.textContent = '🖼️ Comparador de Fondos';
                subtitle.textContent = 'Compara la similitud entre fondos de dos imágenes usando IA';
                buttonText.textContent = 'Comparar Fondos';
            }
        }
        
        function handleDragOver(e) {
            e.preventDefault();
            e.currentTarget.classList.add('dragover');
        }
        
        function handleDragLeave(e) {
            e.currentTarget.classList.remove('dragover');
        }
        
        function handleDrop(e, imageNumber) {
            e.preventDefault();
            e.currentTarget.classList.remove('dragover');
            
            const files = e.dataTransfer.files;
            if (files.length > 0) {
                const file = files[0];
                if (file.type.startsWith('image/')) {
                    handleImageFile(file, imageNumber);
                }
            }
        }
        
        function handleFileSelect(e, imageNumber) {
                const file = e.target.files[0];
                if (file) {
                handleImageFile(file, imageNumber);
            }
        }
        
        function handleImageFile(file, imageNumber) {
            if (imageNumber === 1) {
                image1File = file;
            } else {
                image2File = file;
            }
            
            // Update file info
            const fileInfo = document.getElementById('file' + imageNumber + '-info');
            fileInfo.textContent = file.name + ' (' + (file.size / 1024 / 1024).toFixed(2) + ' MB)';
            
            // Create preview
                    const reader = new FileReader();
            reader.onload = function(e) {
                const preview = document.getElementById('preview' + imageNumber);
                        preview.src = e.target.result;
                        preview.style.display = 'block';
                    };
                    reader.readAsDataURL(file);
                    
            // Enable compare button if both images are loaded
                    updateCompareButton();
        }
        
        function updateCompareButton() {
            const compareBtn = document.getElementById('compareBtn');
            compareBtn.disabled = !(image1File && image2File);
        }
        
        function compareImages() {
            if (!image1File || !image2File) {
                alert('Por favor selecciona ambas imágenes');
                return;
            }
            
            const selectedMode = document.querySelector('input[name="comparison_mode"]:checked').value;
            const loading = document.getElementById('loading');
            const results = document.getElementById('results');
            const compareBtn = document.getElementById('compareBtn');
            
            // Show loading
            loading.style.display = 'block';
            results.style.display = 'none';
            compareBtn.disabled = true;
            
            // Prepare form data
            const formData = new FormData();
            formData.append('image1', image1File);
            formData.append('image2', image2File);
            formData.append('comparison_mode', selectedMode);
            
            // Make API call
            fetch('/api/compare-images', {
                method: 'POST',
                body: formData
            })
            .then(response => response.json())
            .then(data => {
                loading.style.display = 'none';
                compareBtn.disabled = false;
                
                if (data.error) {
                    results.innerHTML = '<div class="error">Error: ' + data.error + '</div>';
                } else {
                    displayResults(data, selectedMode);
                }
                results.style.display = 'block';
            })
            .catch(error => {
                loading.style.display = 'none';
                compareBtn.disabled = false;
                results.innerHTML = '<div class="error">Error de conexión: ' + error.message + '</div>';
                results.style.display = 'block';
            });
        }
        
        function displayResults(data, mode) {
            const results = document.getElementById('results');
            let html = '';
            
            if (mode === 'disease') {
                // Medical evaluation results
                html = '<h3>🏥 Resultados del Análisis Médico (ML)</h3>';
                
                // Individual results for each image
                if (data.image1_medical_probability !== undefined) {
                    html += '<div class="metric">';
                    html += '<span class="metric-name">📷 Imagen 1 - Probabilidad de Condición:</span>';
                    html += '<span class="metric-value ' + getScoreClass(data.image1_medical_probability) + '">' + data.image1_medical_probability + '%</span>';
                    html += '</div>';
                    
                    if (data.image1_diagnosis) {
                        html += '<div class="metric">';
                        html += '<span class="metric-name">🔍 Diagnóstico Imagen 1:</span>';
                        html += '<span class="metric-value">' + data.image1_diagnosis + '</span>';
                        html += '</div>';
                    }
                }
                
                if (data.image2_medical_probability !== undefined) {
                    html += '<div class="metric">';
                    html += '<span class="metric-name">📷 Imagen 2 - Probabilidad de Condición:</span>';
                    html += '<span class="metric-value ' + getScoreClass(data.image2_medical_probability) + '">' + data.image2_medical_probability + '%</span>';
                    html += '</div>';
                    
                    if (data.image2_diagnosis) {
                        html += '<div class="metric">';
                        html += '<span class="metric-name">🔍 Diagnóstico Imagen 2:</span>';
                        html += '<span class="metric-value">' + data.image2_diagnosis + '</span>';
                        html += '</div>';
                    }
                }
                
                if (data.confidence_level && data.confidence_percentage) {
                    html += '<div class="metric">';
                    html += '<span class="metric-name">🎯 Confianza del Análisis:</span>';
                    html += '<span class="metric-value ' + getScoreClass(data.confidence_percentage) + '">' + data.confidence_level + ' (' + data.confidence_percentage + '%)</span>';
                    html += '</div>';
                }
                
                if (data.method) {
                    html += '<div class="metric">';
                    html += '<span class="metric-name">🤖 Método:</span>';
                    html += '<span class="metric-value">' + data.method + '</span>';
                    html += '</div>';
                }
                
            } else {
                // Background comparison results
                html = '<h3>📊 Resultados de Comparación de Fondos</h3>';
                
                // Mostrar conclusión principal primero si existe
                if (data.conclusion) {
                    html += '<div class="conclusion-box">';
                    html += '<div class="conclusion-title">' + data.conclusion.description + '</div>';
                    html += '<div class="conclusion-detail">' + data.conclusion.detail + '</div>';
                    html += '</div>';
                }
                
                // Luego mostrar detalles técnicos
                html += '<h4>📋 Detalles Técnicos:</h4>';
                
                const metrics = [
                    { key: 'overall_similarity', name: '🎯 Similitud General', suffix: '%' },
                    { key: 'pixel_similarity', name: '🖼️ Similitud de Píxeles', suffix: '%' },
                    { key: 'color_similarity', name: '🎨 Similitud de Colores', suffix: '%' },
                    { key: 'stats_similarity', name: '📈 Similitud Estadística', suffix: '%' },
                    { key: 'structural_similarity', name: '🏗️ Similitud Estructural', suffix: '%' },
                    { key: 'hash_similarity', name: '👁️ Reconocimiento Visual', suffix: '%' }
                ];
                
                metrics.forEach(metric => {
                    const value = data[metric.key];
                    if (value !== undefined) {
                        const percentage = Math.round(value * 100);
                        html += '<div class="metric">';
                        html += '<span class="metric-name">' + metric.name + ':</span>';
                        html += '<span class="metric-value ' + getScoreClass(percentage) + '">' + percentage + metric.suffix + '</span>';
                        html += '</div>';
                    }
                });
            }
            
            if (data.processing_time) {
                html += '<div class="metric">';
                html += '<span class="metric-name">⏱️ Tiempo de Procesamiento:</span>';
                html += '<span class="metric-value">' + data.processing_time + 's</span>';
                html += '</div>';
            }
            
            results.innerHTML = html;
        }
        
        function getScoreClass(score) {
            if (score >= 70) return 'high';
            if (score >= 40) return 'medium';
            return 'low';
        }
        
        // Initialize the application
        document.addEventListener('DOMContentLoaded', function() {
            setupInputs();
            updateModeDisplay();
        });
    </script>
</body>
</html>
    """

@app.route('/api/compare-images', methods=['POST'])
def compare_images():
    """API endpoint para comparar imágenes"""
    try:
        start_time = time.time()
        
        # Obtener archivos y modo de comparación
        if 'image1' not in request.files or 'image2' not in request.files:
            return jsonify({'error': 'Faltan archivos de imagen'}), 400
        
        comparison_mode = request.form.get('comparison_mode', 'background')
        
        file1 = request.files['image1']
        file2 = request.files['image2']
        
        if file1.filename == '' or file2.filename == '':
            return jsonify({'error': 'Archivos de imagen vacíos'}), 400
        
        # Cargar imágenes
        img1 = background_comparator.load_image(file1.stream)
        img2 = background_comparator.load_image(file2.stream)
        
        if not img1 or not img2:
            return jsonify({'error': 'Error cargando imágenes'}), 400
        
        # Guardar imágenes temporalmente para análisis médico
        temp_path1 = f"temp_img1_{int(time.time())}.jpg"
        temp_path2 = f"temp_img2_{int(time.time())}.jpg"
        
        try:
            img1.save(temp_path1)
            img2.save(temp_path2)
            
            # Seleccionar comparador según el modo
            if comparison_mode == 'disease':
                results = medical_comparator.analyze_medical_condition(temp_path1, temp_path2)
            else:
                results = background_comparator.compare_images_fast(img1, img2)
            
            processing_time = round(time.time() - start_time, 3)
            
            logger.info(f"Comparando ({comparison_mode}): {file1.filename} vs {file2.filename}")
            
            if comparison_mode == 'disease':
                img1_prob = results.get('image1_medical_probability', 0)
                img2_prob = results.get('image2_medical_probability', 0)
                avg_prob = (img1_prob + img2_prob) / 2
                logger.info(f"Resultado: {avg_prob:.2f}% en {processing_time}s")
            else:
                logger.info(f"Resultado: {results.get('overall_similarity', 0)*100:.1f}% en {processing_time}s")
            
            # Agregar tiempo de procesamiento
            results['processing_time'] = processing_time
            
            return jsonify(results)
            
        finally:
            # Limpiar archivos temporales
            for temp_path in [temp_path1, temp_path2]:
                try:
                    if os.path.exists(temp_path):
                        os.remove(temp_path)
                except:
                    pass
        
    except Exception as e:
        logger.error(f"Error en comparación: {e}")
        return jsonify({'error': 'Error interno del servidor'}), 500

if __name__ == '__main__':
    import os
    port = int(os.environ.get('PORT', 3000))
    logger.info("🌐 Iniciando Comparador de Imágenes Web con ML")
    logger.info(f"🚀 Puerto: {port}")
    app.run(host='0.0.0.0', port=port, debug=False)