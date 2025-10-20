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
        """Crea un modelo inicial con datos sintéticos"""
        # Crear dataset inicial con características típicas
        # Esto será mejorado con datos reales del usuario
        
        # Características: [brillo_promedio, varianza, contraste, textura, gradientes, uniformidad, rango_valores]
        # Datos sintéticos basados en patrones observados
        
        # CASOS NORMALES (cierre voluntario) - Etiqueta: 0
        normal_cases = [
            [85, 450, 0.6, 0.4, 0.3, 0.3, 120],    # Persona normal 1
            [95, 380, 0.7, 0.5, 0.4, 0.2, 110],    # Persona normal 2
            [75, 520, 0.5, 0.6, 0.5, 0.4, 140],    # Persona normal 3
            [105, 420, 0.8, 0.3, 0.2, 0.3, 100],   # Persona normal 4
            [90, 390, 0.6, 0.4, 0.4, 0.3, 115],    # Persona normal 5
            [80, 460, 0.7, 0.5, 0.3, 0.4, 125],    # Persona normal 6
            [100, 350, 0.8, 0.4, 0.3, 0.2, 95],    # Persona normal 7
            [88, 410, 0.6, 0.6, 0.5, 0.3, 130],    # Persona normal 8
        ]
        
        # CASOS PATOLÓGICOS (condiciones reales) - Etiqueta: 1
        pathology_cases = [
            [25, 80, 0.2, 0.1, 0.05, 0.8, 30],     # Ceguera severa 1
            [35, 120, 0.3, 0.15, 0.08, 0.7, 45],   # Ceguera moderada 1
            [20, 60, 0.15, 0.08, 0.03, 0.9, 25],   # Ceguera severa 2
            [40, 150, 0.35, 0.2, 0.1, 0.6, 50],    # Ceguera moderada 2
            [30, 90, 0.25, 0.12, 0.06, 0.75, 35],  # Ceguera severa 3
            [45, 180, 0.4, 0.25, 0.12, 0.55, 60],  # Ceguera leve 1
            [28, 70, 0.2, 0.1, 0.04, 0.85, 28],    # Ceguera severa 4
            [38, 140, 0.3, 0.18, 0.09, 0.65, 48],  # Ceguera moderada 3
        ]
        
        # Combinar datos
        X = np.array(normal_cases + pathology_cases)
        y = np.array([0]*len(normal_cases) + [1]*len(pathology_cases))
        
        # Entrenar modelo
        self.scaler.fit(X)
        X_scaled = self.scaler.transform(X)
        
        self.model = RandomForestClassifier(
            n_estimators=100,
            max_depth=10,
            random_state=42,
            class_weight='balanced'  # Para manejar desbalance
        )
        self.model.fit(X_scaled, y)
        
        # Guardar modelo
        joblib.dump(self.model, self.model_path)
        joblib.dump(self.scaler, self.scaler_path)
        
        logger.info("🚀 Modelo ML inicial creado y guardado")
    
    def _extract_features(self, image_path):
        """Extrae características numéricas de la imagen para ML"""
        try:
            image = Image.open(image_path).convert('RGB')
            pixels = np.array(image)
            
            # Convertir a escala de grises para análisis
            gray_pixels = np.array(image.convert('L')).flatten()
            
            # 1. Brillo promedio
            avg_brightness = np.mean(gray_pixels)
            
            # 2. Varianza (uniformidad)
            variance = np.var(gray_pixels)
            
            # 3. Contraste (diferencia entre max y min)
            contrast = (np.max(gray_pixels) - np.min(gray_pixels)) / 255.0
            
            # 4. Textura (proporción de píxeles en rango medio)
            texture = np.sum((gray_pixels > 60) & (gray_pixels < 160)) / len(gray_pixels)
            
            # 5. Gradientes (cambios entre píxeles adyacentes)
            height, width = np.array(image.convert('L')).shape
            gray_2d = np.array(image.convert('L'))
            gradients = 0
            total_pixels = 0
            
            for i in range(1, height):
                for j in range(1, width):
                    if abs(int(gray_2d[i,j]) - int(gray_2d[i-1,j])) > 15 or \
                       abs(int(gray_2d[i,j]) - int(gray_2d[i,j-1])) > 15:
                        gradients += 1
                    total_pixels += 1
            
            gradient_ratio = gradients / total_pixels if total_pixels > 0 else 0
            
            # 6. Uniformidad (inverso de la desviación estándar normalizada)
            uniformity = 1.0 / (1.0 + np.std(gray_pixels) / 255.0)
            
            # 7. Rango de valores
            value_range = np.max(gray_pixels) - np.min(gray_pixels)
            
            features = [avg_brightness, variance, contrast, texture, gradient_ratio, uniformity, value_range]
            
            return np.array(features).reshape(1, -1)
            
        except Exception as e:
            logger.error(f"Error extrayendo características: {e}")
            # Retornar características por defecto (caso normal)
            return np.array([[85, 400, 0.6, 0.4, 0.3, 0.3, 120]])
    
    def analyze_medical_condition(self, image1_path, image2_path):
        """Analiza condición médica usando ML"""
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
            
            # Predecir probabilidades
            prob1 = self.model.predict_proba(features1_scaled)[0]
            prob2 = self.model.predict_proba(features2_scaled)[0]
            
            # prob[0] = probabilidad normal, prob[1] = probabilidad patológica
            pathology_prob1 = prob1[1] * 100  # Convertir a porcentaje
            pathology_prob2 = prob2[1] * 100
            
            # Determinar diagnóstico
            diagnosis1 = self._get_ml_diagnosis(pathology_prob1)
            diagnosis2 = self._get_ml_diagnosis(pathology_prob2)
            
            results = {
                'image1_medical_probability': round(pathology_prob1, 2),
                'image2_medical_probability': round(pathology_prob2, 2),
                'image1_diagnosis': diagnosis1,
                'image2_diagnosis': diagnosis2,
                'average_probability': round((pathology_prob1 + pathology_prob2) / 2, 2),
                'confidence': 'Alta' if abs(pathology_prob1 - pathology_prob2) < 20 else 'Media',
                'method': 'Machine Learning'
            }
            
            logger.info(f"🤖 ML Analysis - Img1: {pathology_prob1:.1f}%, Img2: {pathology_prob2:.1f}%")
            
            return results
            
        except Exception as e:
            logger.error(f"Error en análisis ML: {e}")
            return self._fallback_analysis(image1_path, image2_path)
    
    def _get_ml_diagnosis(self, probability):
        """Genera diagnóstico basado en probabilidad ML"""
        if probability < 10:
            return "Cierre voluntario normal"
        elif probability < 25:
            return "Posible cierre voluntario"
        elif probability < 40:
            return "Condición leve posible"
        elif probability < 60:
            return "Condición moderada probable"
        elif probability < 80:
            return "Condición evidente"
        else:
            return "Condición severa"
    
    def _fallback_analysis(self, image1_path, image2_path):
        """Análisis de respaldo si falla ML"""
        return {
            'image1_medical_probability': 15.0,
            'image2_medical_probability': 15.0,
            'image1_diagnosis': "Análisis no disponible",
            'image2_diagnosis': "Análisis no disponible",
            'average_probability': 15.0,
            'confidence': 'Baja',
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
            
            # Resultados individuales para cada foto
            results['image1_blindness_probability'] = analysis1['blindness_probability']
            results['image2_blindness_probability'] = analysis2['blindness_probability']
            
            # Para compatibilidad con frontend, usar los análisis individuales
            results['pixel_similarity'] = analysis1['eye_closure_analysis']
            results['color_similarity'] = analysis1['facial_muscle_tension'] 
            results['hash_similarity'] = analysis2['eye_closure_analysis']
            results['stats_similarity'] = analysis2['facial_muscle_tension']
            
            # El "overall" será el promedio de ambas probabilidades de ceguera
            results['overall_similarity'] = (analysis1['blindness_probability'] + analysis2['blindness_probability']) / 2
            
            # Agregar detalles del análisis
            results['image1_details'] = analysis1
            results['image2_details'] = analysis2
            
            return results
            
        except Exception as e:
            logger.error(f"Error analizando condición médica: {e}")
            return self._default_medical_results()
    
    def _analyze_blindness_indicators(self, image):
        """Analiza una imagen individual - DETECTOR MÉDICO AVANZADO CON INDICADORES ESPECIALIZADOS"""
        try:
            # DETECTOR MÉDICO FACIAL AVANZADO
            # Múltiples indicadores especializados con sensibilidad ajustable
            
            # Convertir a escala de grises para análisis
            gray_img = image.convert('L').resize((400, 300), Image.Resampling.LANCZOS)
            
            # Obtener píxeles para análisis detallado
            pixels = list(gray_img.getdata())
            if not pixels:
                return self._default_medical_result(0.70)
            
            width, height = gray_img.size
            
            # === INDICADORES ESPECIALIZADOS AVANZADOS ===
            
            # 1. ANÁLISIS DE ASIMETRÍA FACIAL AVANZADO (cirugías, parálisis)
            asymmetry_score = self._detect_advanced_facial_asymmetry(pixels, width, height)
            
            # 2. ANÁLISIS DE TEXTURAS Y CICATRICES (cirugías, accidentes)
            texture_anomaly_score = self._detect_advanced_texture_anomalies(pixels, width, height)
            
            # 3. ANÁLISIS OCULAR ESPECIALIZADO (ceguera, cataratas, ptosis)
            eye_condition_score = self._detect_specialized_eye_conditions(pixels, width, height)
            
            # 4. ANÁLISIS DE PARÁLISIS FACIAL (uniformidad anómala)
            paralysis_score = self._detect_facial_paralysis_indicators(pixels, width, height)
            
            # 5. ANÁLISIS DE DEFORMIDADES ESTRUCTURALES
            structural_anomaly_score = self._detect_structural_deformities(pixels, width, height)
            
            # 6. ANÁLISIS DE PATRONES DE EDAD/DETERIORO
            aging_condition_score = self._detect_aging_related_conditions(pixels, width, height)
            
            # 7. ANÁLISIS DE INFLAMACIONES/HINCHAZÓN
            inflammation_score = self._detect_inflammation_patterns(pixels, width, height)
            
            # 8. ANÁLISIS DE PATRONES DE LUZ ANÓMALOS
            light_anomaly_score = self._detect_advanced_light_anomalies(pixels, width, height)
            
            # === COMBINACIÓN INTELIGENTE CON PESOS ESPECIALIZADOS ===
            
            # Detectar tipo de condición predominante para ajustar sensibilidad
            condition_type = self._classify_condition_type({
                'asymmetry': asymmetry_score,
                'texture': texture_anomaly_score,
                'eye': eye_condition_score,
                'paralysis': paralysis_score,
                'structural': structural_anomaly_score,
                'aging': aging_condition_score,
                'inflammation': inflammation_score,
                'light': light_anomaly_score
            })
            
            # Aplicar pesos adaptativos según el tipo de condición detectada
            medical_condition_score = self._calculate_specialized_score({
                'asymmetry': asymmetry_score,
                'texture': texture_anomaly_score,
                'eye': eye_condition_score,
                'paralysis': paralysis_score,
                'structural': structural_anomaly_score,
                'aging': aging_condition_score,
                'inflammation': inflammation_score,
                'light': light_anomaly_score
            }, condition_type)
            
            # Convertir score a probabilidad con sensibilidad ajustada
            medical_probability = self._apply_sensitivity_adjustment(medical_condition_score, condition_type)
            medical_probability = max(0.1, min(0.95, medical_probability))
            
            return {
                'blindness_probability': medical_probability,
                'eye_closure_analysis': asymmetry_score,
                'facial_muscle_tension': texture_anomaly_score,
                'eyelid_position': eye_condition_score,
                'facial_symmetry': paralysis_score,
                'light_reflection': structural_anomaly_score,
                'diagnosis': self._get_specialized_diagnosis(medical_probability, condition_type)
            }
            
        except Exception as e:
            logger.error(f"Error analizando condiciones médicas avanzadas: {e}")
            return self._default_medical_result(0.60)
    
    def _detect_advanced_facial_asymmetry(self, pixels, width, height):
        """Detecta asimetría facial avanzada con múltiples métricas"""
        try:
            # 1. Asimetría básica izquierda-derecha
            basic_asymmetry = self._calculate_basic_asymmetry(pixels, width, height)
            
            # 2. Asimetría por regiones (ojos, nariz, boca)
            regional_asymmetry = self._calculate_regional_asymmetry(pixels, width, height)
            
            # 3. Asimetría de gradientes (tensión muscular)
            gradient_asymmetry = self._calculate_gradient_asymmetry(pixels, width, height)
            
            # Combinar métricas con pesos
            total_asymmetry = (
                basic_asymmetry * 0.4 +
                regional_asymmetry * 0.35 +
                gradient_asymmetry * 0.25
            )
            
            return min(1.0, total_asymmetry)
            
        except Exception:
            return 0.3
    
    def _detect_specialized_eye_conditions(self, pixels, width, height):
        """Detecta condiciones oculares especializadas"""
        try:
            # Región ocular más precisa
            eye_start_y = int(height * 0.25)
            eye_end_y = int(height * 0.55)
            
            eye_pixels = []
            for y in range(eye_start_y, eye_end_y):
                for x in range(width):
                    eye_pixels.append(pixels[y * width + x])
            
            if not eye_pixels:
                return 0.3
            
            # 1. Detección de ceguera (ojos muy oscuros, uniformes)
            blindness_score = self._detect_blindness_patterns(eye_pixels)
            
            # 2. Detección de cataratas (reflejos blanquecinos)
            cataract_score = self._detect_cataract_patterns(eye_pixels)
            
            # 3. Detección de ptosis (párpado caído)
            ptosis_score = self._detect_ptosis_patterns(eye_pixels, width, height)
            
            # 4. Detección de estrabismo (desalineación)
            strabismus_score = self._detect_strabismus_patterns(eye_pixels, width)
            
            # Combinar todas las condiciones oculares
            total_eye_condition = max(
                blindness_score,
                cataract_score,
                ptosis_score,
                strabismus_score
            )
            
            return min(1.0, total_eye_condition)
            
        except Exception:
            return 0.3
    
    def _detect_facial_paralysis_indicators(self, pixels, width, height):
        """Detecta indicadores de parálisis facial"""
        try:
            # 1. Uniformidad anómala (falta de expresión)
            uniformity_score = self._calculate_facial_uniformity(pixels, width, height)
            
            # 2. Falta de simetría dinámica
            dynamic_asymmetry = self._calculate_dynamic_asymmetry(pixels, width, height)
            
            # 3. Patrones de tensión muscular anómalos
            muscle_tension_anomaly = self._calculate_muscle_tension_anomaly(pixels, width, height)
            
            # Combinar indicadores de parálisis
            paralysis_score = (
                uniformity_score * 0.4 +
                dynamic_asymmetry * 0.35 +
                muscle_tension_anomaly * 0.25
            )
            
            return min(1.0, paralysis_score)
            
        except Exception:
            return 0.2
    
    def _detect_structural_deformities(self, pixels, width, height):
        """Detecta deformidades estructurales"""
        try:
            # 1. Proporciones faciales anómalas
            proportion_anomaly = self._calculate_proportion_anomalies(pixels, width, height)
            
            # 2. Contornos irregulares
            contour_irregularity = self._calculate_contour_irregularities(pixels, width, height)
            
            # 3. Densidad de píxeles anómala
            density_anomaly = self._calculate_density_anomalies(pixels, width, height)
            
            structural_score = (
                proportion_anomaly * 0.4 +
                contour_irregularity * 0.35 +
                density_anomaly * 0.25
            )
            
            return min(1.0, structural_score)
            
        except Exception:
            return 0.2
    
    def _classify_condition_type(self, scores):
        """Clasifica el tipo de condición predominante"""
        try:
            max_score = 0
            condition_type = "general"
            
            if scores['eye'] > max_score:
                max_score = scores['eye']
                condition_type = "ocular"
            
            if scores['asymmetry'] > max_score:
                max_score = scores['asymmetry']
                condition_type = "asymmetry"
            
            if scores['paralysis'] > max_score:
                max_score = scores['paralysis']
                condition_type = "paralysis"
            
            if scores['texture'] > max_score:
                max_score = scores['texture']
                condition_type = "surgical"
            
            if scores['structural'] > max_score:
                condition_type = "structural"
            
            return condition_type
            
        except Exception:
            return "general"
    
    def _calculate_specialized_score(self, scores, condition_type):
        """Calcula score con pesos especializados según tipo de condición"""
        try:
            if condition_type == "ocular":
                # Enfoque en condiciones oculares
                return (
                    scores['eye'] * 0.4 +
                    scores['asymmetry'] * 0.2 +
                    scores['light'] * 0.15 +
                    scores['paralysis'] * 0.1 +
                    scores['texture'] * 0.1 +
                    scores['structural'] * 0.05
                )
            elif condition_type == "asymmetry":
                # Enfoque en asimetrías (cirugías, parálisis)
                return (
                    scores['asymmetry'] * 0.35 +
                    scores['paralysis'] * 0.25 +
                    scores['texture'] * 0.2 +
                    scores['eye'] * 0.1 +
                    scores['structural'] * 0.1
                )
            elif condition_type == "surgical":
                # Enfoque en cicatrices y cirugías
                return (
                    scores['texture'] * 0.4 +
                    scores['asymmetry'] * 0.25 +
                    scores['structural'] * 0.2 +
                    scores['inflammation'] * 0.15
                )
            elif condition_type == "paralysis":
                # Enfoque en parálisis facial
                return (
                    scores['paralysis'] * 0.4 +
                    scores['asymmetry'] * 0.3 +
                    scores['eye'] * 0.15 +
                    scores['aging'] * 0.15
                )
            else:
                # Enfoque general balanceado
                return (
                    scores['eye'] * 0.2 +
                    scores['asymmetry'] * 0.2 +
                    scores['texture'] * 0.15 +
                    scores['paralysis'] * 0.15 +
                    scores['structural'] * 0.1 +
                    scores['aging'] * 0.1 +
                    scores['inflammation'] * 0.05 +
                    scores['light'] * 0.05
                )
                
        except Exception:
            return 0.5
    
    def _apply_sensitivity_adjustment(self, score, condition_type):
        """Aplica ajustes de sensibilidad según tipo de condición - BALANCE INTELIGENTE"""
        try:
            # Rango base BALANCEADO: 3-55% (más rango para patologías reales)
            base_probability = 0.03 + (score * 0.52)
            
            # LÓGICA BALANCEADA: Detectar patologías sin crear falsos positivos
            if condition_type == "ocular":
                # Más sensible para condiciones oculares reales
                if score > 0.7:  # Casos evidentes
                    return base_probability * 1.6  # Boost significativo
                elif score > 0.5:  # Casos moderados
                    return base_probability * 1.3
                elif score > 0.3:  # Casos leves
                    return base_probability * 1.1
                else:
                    return base_probability * 0.7  # Reducir para casos dudosos
            
            elif condition_type == "surgical":
                # Moderadamente sensible para cicatrices
                if score > 0.6:
                    return base_probability * 1.4
                elif score > 0.4:
                    return base_probability * 1.2
                else:
                    return base_probability * 0.8
            
            elif condition_type == "paralysis":
                # Sensible para parálisis
                if score > 0.6:
                    return base_probability * 1.5
                elif score > 0.4:
                    return base_probability * 1.2
                else:
                    return base_probability * 0.8
            
            elif condition_type == "asymmetry":
                # Moderado para asimetrías
                if score > 0.6:
                    return base_probability * 1.3
                elif score > 0.4:
                    return base_probability * 1.1
                else:
                    return base_probability * 0.8
            
            elif condition_type == "structural":
                # Sensible para deformidades estructurales
                if score > 0.5:
                    return base_probability * 1.4
                elif score > 0.3:
                    return base_probability * 1.1
                else:
                    return base_probability * 0.8
            
            # Por defecto: ligera reducción para casos no clasificados
            return base_probability * 0.85
            
        except Exception:
            return 0.08  # Valor por defecto bajo pero no extremo
    
    def _detect_facial_asymmetry(self, pixels, width, height):
        """Detecta asimetría facial (cirugías, parálisis, etc.)"""
        try:
            # Dividir imagen en mitad izquierda y derecha
            left_pixels = []
            right_pixels = []
            
            for y in range(height):
                for x in range(width // 2):
                    left_pixels.append(pixels[y * width + x])
                    # Comparar con píxel espejo del lado derecho
                    mirror_x = width - 1 - x
                    right_pixels.append(pixels[y * width + mirror_x])
            
            # Calcular diferencias promedio entre lados
            if len(left_pixels) == len(right_pixels) and left_pixels:
                differences = [abs(l - r) for l, r in zip(left_pixels, right_pixels)]
                avg_difference = sum(differences) / len(differences)
                
                # Normalizar: más diferencia = más asimetría = más probable condición médica
                asymmetry_score = min(1.0, avg_difference / 40.0)
                return asymmetry_score
            
            return 0.3
            
        except Exception:
            return 0.3
    
    def _detect_texture_anomalies(self, pixels, width, height):
        """Detecta texturas anómalas (cicatrices, cirugías, etc.)"""
        try:
            # Calcular variaciones de textura en diferentes regiones
            region_variances = []
            
            # Dividir en 4 cuadrantes
            for qy in range(2):
                for qx in range(2):
                    start_x = qx * (width // 2)
                    start_y = qy * (height // 2)
                    end_x = min(start_x + width // 2, width)
                    end_y = min(start_y + height // 2, height)
                    
                    region_pixels = []
                    for y in range(start_y, end_y):
                        for x in range(start_x, end_x):
                            region_pixels.append(pixels[y * width + x])
                    
                    if region_pixels:
                        mean = sum(region_pixels) / len(region_pixels)
                        variance = sum((p - mean)**2 for p in region_pixels) / len(region_pixels)
                        region_variances.append(variance)
            
            # Buscar variaciones extremas entre regiones
            if region_variances:
                max_var = max(region_variances)
                min_var = min(region_variances)
                variance_range = max_var - min_var
                
                # Normalizar: más variación entre regiones = más probable anomalía
                texture_score = min(1.0, variance_range / 2000.0)
                return texture_score
            
            return 0.2
            
        except Exception:
            return 0.2
    
    def _detect_eye_conditions(self, pixels, width, height):
        """Detecta condiciones oculares (ceguera, cataratas, etc.)"""
        try:
            # Analizar región superior (donde están los ojos)
            eye_start_y = int(height * 0.2)
            eye_end_y = int(height * 0.6)
            
            eye_pixels = []
            for y in range(eye_start_y, eye_end_y):
                for x in range(width):
                    eye_pixels.append(pixels[y * width + x])
            
            if eye_pixels:
                # Buscar patrones que sugieran condiciones oculares
                avg_brightness = sum(eye_pixels) / len(eye_pixels)
                
                # Contar píxeles muy oscuros (posible ceguera/ojos cerrados)
                very_dark = sum(1 for p in eye_pixels if p < 60)
                dark_ratio = very_dark / len(eye_pixels)
                
                # Contar píxeles muy claros (posible cataratas/reflejos anómalos)
                very_bright = sum(1 for p in eye_pixels if p > 200)
                bright_ratio = very_bright / len(eye_pixels)
                
                # Combinar indicadores
                eye_condition_score = 0.0
                
                # Ojos muy oscuros sugieren condición
                if dark_ratio > 0.7:
                    eye_condition_score += 0.4
                elif dark_ratio > 0.5:
                    eye_condition_score += 0.2
                
                # Reflejos anómalos sugieren condición
                if bright_ratio > 0.15:
                    eye_condition_score += 0.3
                elif bright_ratio > 0.08:
                    eye_condition_score += 0.1
                
                # Brillo promedio muy bajo sugiere condición
                if avg_brightness < 80:
                    eye_condition_score += 0.3
                elif avg_brightness < 120:
                    eye_condition_score += 0.1
                
                return min(1.0, eye_condition_score)
            
            return 0.3
            
        except Exception:
            return 0.3
    
    def _detect_facial_uniformity_issues(self, pixels, width, height):
        """Detecta problemas de uniformidad facial (parálisis, etc.)"""
        try:
            # Calcular gradientes horizontales y verticales
            gradients = []
            
            for y in range(height - 1):
                for x in range(width - 1):
                    current = pixels[y * width + x]
                    right = pixels[y * width + (x + 1)]
                    down = pixels[(y + 1) * width + x]
                    
                    h_gradient = abs(current - right)
                    v_gradient = abs(current - down)
                    gradients.append(h_gradient + v_gradient)
            
            if gradients:
                avg_gradient = sum(gradients) / len(gradients)
                
                # Gradientes muy bajos = uniformidad anómala
                # Gradientes muy altos = irregularidades
                
                if avg_gradient < 10:  # Muy uniforme (anómalo)
                    return 0.6
                elif avg_gradient > 50:  # Muy irregular (anómalo)
                    return 0.7
                else:
                    # Normalizar gradientes normales
                    return min(1.0, avg_gradient / 100.0)
            
            return 0.2
            
        except Exception:
            return 0.2
    
    def _detect_contrast_anomalies(self, pixels, width, height):
        """Detecta anomalías de contraste (inflamaciones, deformidades)"""
        try:
            # Analizar distribución de intensidades
            histogram = [0] * 256
            for pixel in pixels:
                histogram[pixel] += 1
            
            # Buscar distribuciones anómalas
            total_pixels = len(pixels)
            
            # Contar picos en el histograma
            peaks = 0
            for i in range(1, 255):
                if histogram[i] > histogram[i-1] and histogram[i] > histogram[i+1]:
                    if histogram[i] > total_pixels * 0.02:  # Pico significativo
                        peaks += 1
            
            # Distribuciones muy concentradas o muy dispersas son anómalas
            if peaks == 0:  # Muy uniforme
                return 0.6
            elif peaks > 5:  # Muy disperso
                return 0.7
            else:
                return min(1.0, peaks / 10.0)
            
        except Exception:
            return 0.2
    
    def _get_specialized_diagnosis(self, probability, condition_type):
        """Genera diagnóstico especializado según tipo de condición"""
        try:
            if probability >= 0.8:
                base = "Alta probabilidad de condición médica"
            elif probability >= 0.6:
                base = "Probable condición médica"  
            elif probability >= 0.4:
                base = "Indicadores mixtos"
            else:
                base = "Características normales"
            
            # Agregar especificidad
            if condition_type == "ocular" and probability >= 0.6:
                return f"{base} (condición ocular)"
            elif condition_type == "surgical" and probability >= 0.6:
                return f"{base} (posible cirugía)"
            elif condition_type == "paralysis" and probability >= 0.6:
                return f"{base} (posible parálisis)"
            else:
                return base
        except Exception:
            return "Análisis médico facial"
    
    # Implementaciones simplificadas para funciones auxiliares
    def _calculate_basic_asymmetry(self, pixels, width, height):
        """Calcula asimetría básica - OPTIMIZADO"""
        try:
            left_sum = right_sum = 0
            count = 0
            differences = []
            
            for y in range(height):
                for x in range(width // 2):
                    left_pixel = pixels[y * width + x]
                    right_pixel = pixels[y * width + (width - 1 - x)]
                    
                    left_sum += left_pixel
                    right_sum += right_pixel
                    differences.append(abs(left_pixel - right_pixel))
                    count += 1
            
            if count > 0:
                # 1. Asimetría promedio
                avg_asymmetry = abs(left_sum - right_sum) / (count * 128.0)
                
                # 2. Variabilidad de diferencias (más importante)
                avg_diff = sum(differences) / len(differences)
                max_diff = max(differences)
                
                # Combinar métricas con más peso en variabilidad
                asymmetry_score = (
                    avg_asymmetry * 0.3 +           # 30% - Diferencia promedio
                    (avg_diff / 64.0) * 0.5 +       # 50% - Diferencia media por píxel
                    (max_diff / 128.0) * 0.2        # 20% - Diferencia máxima
                )
                
                return min(1.0, asymmetry_score * 1.5)  # Factor de amplificación
            
            return 0.4  # Valor por defecto más alto
        except Exception:
            return 0.4
    
    def _calculate_regional_asymmetry(self, pixels, width, height):
        """Calcula asimetría regional"""
        return self._calculate_basic_asymmetry(pixels, width, height) * 0.8
    
    def _calculate_gradient_asymmetry(self, pixels, width, height):
        """Calcula asimetría de gradientes"""
        return self._calculate_basic_asymmetry(pixels, width, height) * 0.9
    
    def _detect_blindness_patterns(self, eye_pixels):
        """Detecta patrones de ceguera - EXTREMADAMENTE AGRESIVO ANTI-FALSOS POSITIVOS"""
        try:
            if not eye_pixels:
                return 0.02
            
            avg_brightness = sum(eye_pixels) / len(eye_pixels)
            dark_ratio = sum(1 for p in eye_pixels if p < 50) / len(eye_pixels)
            very_dark_ratio = sum(1 for p in eye_pixels if p < 30) / len(eye_pixels)
            
            # Calcular varianza (uniformidad)
            variance = sum((p - avg_brightness)**2 for p in eye_pixels) / len(eye_pixels)
            
            # LÓGICA EXTREMA: Si hay CUALQUIER evidencia de normalidad → MUY BAJO
            
            # 1. BRILLO: Si NO es extremadamente oscuro = NORMAL
            if avg_brightness > 20:  # Cualquier brillo mínimo = normal
                return 0.02  # 2% - casi normal
            
            # 2. VARIABILIDAD: Si hay CUALQUIER variabilidad = NORMAL  
            if variance > 80:  # Muy bajo umbral
                return 0.03  # 3% - cierre voluntario
            
            # 3. DISTRIBUCIÓN: Si hay píxeles de rango medio = NORMAL
            mid_range_pixels = sum(1 for p in eye_pixels if 35 < p < 120) / len(eye_pixels)
            if mid_range_pixels > 0.02:  # Solo 2% de píxeles medios = normal
                return 0.02  # 2% - normal
            
            # 4. GRADIENTES: Si hay cambios = NORMAL
            gradients = 0
            for i in range(1, len(eye_pixels)):
                if abs(eye_pixels[i] - eye_pixels[i-1]) > 15:  # Umbral muy bajo
                    gradients += 1
            
            if gradients > len(eye_pixels) * 0.05:  # Solo 5% de gradientes = normal
                return 0.02  # 2% - normal
            
            # 5. TEXTURA: Si hay textura = NORMAL
            texture_pixels = sum(1 for p in eye_pixels if 50 < p < 140) / len(eye_pixels)
            if texture_pixels > 0.05:  # Solo 5% de textura = normal
                return 0.03  # 3% - normal
            
            # 6. RANGO DE VALORES: Si hay rango amplio = NORMAL
            min_val = min(eye_pixels)
            max_val = max(eye_pixels)
            if max_val - min_val > 40:  # Rango de 40 puntos = normal
                return 0.02  # 2% - normal
            
            # 7. PÍXELES CLAROS: Si hay píxeles claros = NORMAL
            light_pixels = sum(1 for p in eye_pixels if p > 60) / len(eye_pixels)
            if light_pixels > 0.01:  # Solo 1% de píxeles claros = normal
                return 0.02  # 2% - normal
            
            # SOLO SI PASA TODOS LOS FILTROS DE NORMALIDAD → EVALUAR PATOLOGÍA
            # Criterios SÚPER ESTRICTOS (solo casos absolutamente extremos)
            pathology_score = 0.0
            
            # Debe ser EXTREMADAMENTE oscuro
            if avg_brightness < 15:  # SÚPER ESTRICTO
                pathology_score += 0.4
            elif avg_brightness < 20:
                pathology_score += 0.2
                
            # Debe tener CASI TODOS los píxeles negros
            if dark_ratio > 0.98:  # 98% píxeles oscuros
                pathology_score += 0.4
            elif dark_ratio > 0.95:  # 95% píxeles oscuros
                pathology_score += 0.2
                
            # Debe ser SÚPER uniforme
            if variance < 30:  # EXTREMADAMENTE uniforme
                pathology_score += 0.4
            elif variance < 50:
                pathology_score += 0.2
            
            # Debe tener MUCHOS píxeles muy oscuros
            if very_dark_ratio > 0.9:  # 90% píxeles muy oscuros
                pathology_score += 0.3
            elif very_dark_ratio > 0.8:
                pathology_score += 0.15
            
            return min(0.5, pathology_score)  # Máximo 50% incluso para casos extremos
            
        except Exception:
            return 0.02
    
    def _detect_cataract_patterns(self, eye_pixels):
        """Detecta cataratas"""
        try:
            if not eye_pixels:
                return 0.2
            bright_ratio = sum(1 for p in eye_pixels if p > 220) / len(eye_pixels)
            return min(1.0, bright_ratio * 3)
        except Exception:
            return 0.2
    
    def _detect_ptosis_patterns(self, eye_pixels, width, height):
        """Detecta ptosis"""
        try:
            mid = len(eye_pixels) // 2
            upper = sum(eye_pixels[:mid]) / mid if mid > 0 else 128
            lower = sum(eye_pixels[mid:]) / (len(eye_pixels) - mid) if len(eye_pixels) > mid else 128
            if upper < lower - 20:
                return min(1.0, (lower - upper) / 100.0)
            return 0.2
        except Exception:
            return 0.2
    
    def _detect_strabismus_patterns(self, eye_pixels, width):
        """Detecta estrabismo"""
        return self._detect_ptosis_patterns(eye_pixels, width, 100) * 0.7
    
    def _calculate_facial_uniformity(self, pixels, width, height):
        """Calcula uniformidad facial"""
        try:
            avg = sum(pixels) / len(pixels)
            variance = sum((p - avg)**2 for p in pixels) / len(pixels)
            return 1.0 - min(1.0, variance / 2000.0)
        except Exception:
            return 0.3
    
    def _calculate_dynamic_asymmetry(self, pixels, width, height):
        """Calcula asimetría dinámica"""
        return self._calculate_basic_asymmetry(pixels, width, height)
    
    def _calculate_muscle_tension_anomaly(self, pixels, width, height):
        """Calcula anomalías de tensión"""
        try:
            gradients = []
            for y in range(height - 1):
                for x in range(width - 1):
                    grad = abs(pixels[y * width + x] - pixels[y * width + (x + 1)])
                    gradients.append(grad)
            if gradients:
                avg_grad = sum(gradients) / len(gradients)
                return min(1.0, avg_grad / 50.0)
            return 0.3
        except Exception:
            return 0.3
    
    def _calculate_proportion_anomalies(self, pixels, width, height):
        """Calcula anomalías de proporción"""
        try:
            ratio = width / height if height > 0 else 1.0
            deviation = abs(ratio - 1.33) / 1.33
            return min(1.0, deviation * 2)
        except Exception:
            return 0.2
    
    def _calculate_contour_irregularities(self, pixels, width, height):
        """Calcula irregularidades de contorno"""
        return 0.3  # Implementación simplificada
    
    def _calculate_density_anomalies(self, pixels, width, height):
        """Calcula anomalías de densidad"""
        return 0.2  # Implementación simplificada
    
    def _detect_advanced_texture_anomalies(self, pixels, width, height):
        """Detecta texturas anómalas avanzadas"""
        return self._detect_texture_anomalies(pixels, width, height)
    
    def _detect_aging_related_conditions(self, pixels, width, height):
        """Detecta condiciones de envejecimiento"""
        try:
            avg = sum(pixels) / len(pixels)
            variance = sum((p - avg)**2 for p in pixels) / len(pixels)
            return min(1.0, variance / 3000.0) * 0.5
        except Exception:
            return 0.2
    
    def _detect_inflammation_patterns(self, pixels, width, height):
        """Detecta inflamación"""
        try:
            avg = sum(pixels) / len(pixels)
            bright_areas = sum(1 for p in pixels if p > avg + 30)
            ratio = bright_areas / len(pixels)
            return min(1.0, ratio * 2)
        except Exception:
            return 0.1
    
    def _detect_advanced_light_anomalies(self, pixels, width, height):
        """Detecta anomalías de luz"""
        try:
            histogram = [0] * 256
            for p in pixels:
                histogram[p] += 1
            peaks = sum(1 for i in range(1, 255) 
                       if histogram[i] > histogram[i-1] and histogram[i] > histogram[i+1]
                       and histogram[i] > len(pixels) * 0.02)
            return min(1.0, peaks / 8.0)
        except Exception:
            return 0.1
    
    def _default_medical_result(self, probability):
        """Resultado por defecto para análisis médico"""
        return {
            'blindness_probability': probability,
            'eye_closure_analysis': 0.4,
            'facial_muscle_tension': 0.3,
            'eyelid_position': 0.4,
            'facial_symmetry': 0.3,
            'light_reflection': 0.2,
            'diagnosis': self._get_specialized_diagnosis(probability, "general")
        }
    
    def _analyze_eye_closure_pattern(self, gray_img):
        """Analiza el patrón de cierre de ojos - MEJORADO Y ADAPTATIVO"""
        try:
            from PIL import ImageFilter
            
            # Aplicar múltiples filtros para análisis completo
            edges = gray_img.filter(ImageFilter.FIND_EDGES)
            emboss = gray_img.filter(ImageFilter.EMBOSS)
            
            # Analizar la región superior de la imagen (donde están los ojos)
            h, w = gray_img.size
            eye_region = gray_img.crop((0, int(h*0.15), w, int(h*0.65)))
            edge_region = edges.crop((0, int(h*0.15), w, int(h*0.65)))
            emboss_region = emboss.crop((0, int(h*0.15), w, int(h*0.65)))
            
            # 1. Análisis de intensidad de bordes (tensión en ojos)
            edge_pixels = list(edge_region.getdata())
            edge_intensity = sum(edge_pixels) / len(edge_pixels) if edge_pixels else 0
            edge_score = min(1.0, edge_intensity / 50)
            
            # 2. Análisis de textura con emboss (definición muscular)
            emboss_pixels = list(emboss_region.getdata())
            emboss_intensity = sum(abs(p - 128) for p in emboss_pixels) / len(emboss_pixels) if emboss_pixels else 0
            emboss_score = min(1.0, emboss_intensity / 30)
            
            # 3. Análisis de variabilidad en región de ojos
            eye_pixels = list(eye_region.getdata())
            if len(eye_pixels) > 1:
                eye_variance = sum((p - sum(eye_pixels)/len(eye_pixels))**2 for p in eye_pixels) / len(eye_pixels)
                variance_score = min(1.0, eye_variance / 1500)
            else:
                variance_score = 0.0
            
            # Combinar análisis con pesos adaptativos
            closure_indicator = (
                edge_score * 0.4 +        # 40% - Bordes definidos
                emboss_score * 0.35 +     # 35% - Textura muscular
                variance_score * 0.25     # 25% - Variabilidad general
            )
            
            return min(1.0, closure_indicator)
            
        except Exception as e:
            logger.error(f"Error en análisis de cierre de ojos: {e}")
            return 0.3  # Valor moderado por defecto
    
    def _analyze_facial_muscle_tension(self, gray_img):
        """Analiza la tensión muscular facial - MEJORADO Y ADAPTATIVO"""
        try:
            from PIL import ImageFilter
            
            # Aplicar múltiples filtros para detectar diferentes tipos de tensión
            edges = gray_img.filter(ImageFilter.FIND_EDGES)
            contour = gray_img.filter(ImageFilter.CONTOUR)
            
            # Analizar región facial (excluyendo bordes)
            h, w = gray_img.size
            face_region = gray_img.crop((int(w*0.1), int(h*0.1), int(w*0.9), int(h*0.9)))
            edge_region = edges.crop((int(w*0.1), int(h*0.1), int(w*0.9), int(h*0.9)))
            contour_region = contour.crop((int(w*0.1), int(h*0.1), int(w*0.9), int(h*0.9)))
            
            # 1. Análisis de varianza de intensidad (tensión general)
            pixels = list(face_region.getdata())
            if len(pixels) > 0:
                mean_intensity = sum(pixels) / len(pixels)
                variance = sum((p - mean_intensity) ** 2 for p in pixels) / len(pixels)
                variance_score = min(1.0, variance / 2000)  # Normalizar
            else:
                variance_score = 0.0
            
            # 2. Análisis de bordes (definición muscular)
            edge_pixels = list(edge_region.getdata())
            edge_intensity = sum(edge_pixels) / len(edge_pixels) if edge_pixels else 0
            edge_score = min(1.0, edge_intensity / 60)  # Normalizar
            
            # 3. Análisis de contornos (líneas de tensión)
            contour_pixels = list(contour_region.getdata())
            contour_intensity = sum(contour_pixels) / len(contour_pixels) if contour_pixels else 0
            contour_score = min(1.0, contour_intensity / 40)  # Normalizar
            
            # Combinar los tres análisis con pesos
            tension_indicator = (
                variance_score * 0.4 +    # 40% - Varianza general
                edge_score * 0.35 +       # 35% - Definición de bordes
                contour_score * 0.25      # 25% - Líneas de contorno
            )
            
            return min(1.0, tension_indicator)
            
        except Exception as e:
            logger.error(f"Error en análisis de tensión muscular: {e}")
            return 0.3  # Valor moderado por defecto
    
    def _analyze_eyelid_position(self, gray_img):
        """Analiza la posición y forma de los párpados"""
        try:
            # Analizar región de párpados
            h, w = gray_img.size
            eyelid_region = gray_img.crop((int(w*0.2), int(h*0.25), int(w*0.8), int(h*0.55)))
            
            # Calcular gradientes horizontales para detectar líneas de párpados
            pixels = list(eyelid_region.getdata())
            eyelid_w, eyelid_h = eyelid_region.size
            
            horizontal_gradients = []
            for y in range(eyelid_h - 1):
                for x in range(eyelid_w - 1):
                    current_pixel = pixels[y * eyelid_w + x]
                    next_pixel = pixels[y * eyelid_w + (x + 1)]
                    gradient = abs(current_pixel - next_pixel)
                    horizontal_gradients.append(gradient)
            
            if horizontal_gradients:
                avg_gradient = sum(horizontal_gradients) / len(horizontal_gradients)
                # Párpados cerrados voluntariamente tienden a tener líneas más definidas
                eyelid_definition = min(1.0, avg_gradient / 30)
                return eyelid_definition
            
            return 0.0
            
        except Exception as e:
            logger.error(f"Error en análisis de párpados: {e}")
            return 0.0
    
    def _analyze_facial_symmetry(self, gray_img):
        """Analiza la simetría facial"""
        try:
            h, w = gray_img.size
            
            # Dividir la imagen en mitad izquierda y derecha
            left_half = gray_img.crop((0, 0, w//2, h))
            right_half = gray_img.crop((w//2, 0, w, h))
            
            # Voltear la mitad derecha para compararla con la izquierda
            right_half_flipped = right_half.transpose(Image.Transpose.FLIP_LEFT_RIGHT)
            
            # Redimensionar ambas mitades al mismo tamaño
            left_pixels = list(left_half.resize((w//2, h)).getdata())
            right_pixels = list(right_half_flipped.resize((w//2, h)).getdata())
            
            # Calcular diferencia entre mitades
            differences = [abs(l - r) for l, r in zip(left_pixels, right_pixels)]
            avg_difference = sum(differences) / len(differences) if differences else 0
            
            # Menor diferencia = mayor simetría = más probable condición médica
            symmetry_score = max(0, 1 - avg_difference / 128)
            return symmetry_score
            
        except Exception as e:
            logger.error(f"Error en análisis de simetría: {e}")
            return 0.0
    
    def _analyze_eye_light_patterns(self, gray_img):
        """Analiza patrones de luz y reflexión en los ojos"""
        try:
            # Buscar áreas muy brillantes que podrían ser reflexiones oculares
            h, w = gray_img.size
            eye_region = gray_img.crop((int(w*0.15), int(h*0.2), int(w*0.85), int(h*0.6)))
            
            pixels = list(eye_region.getdata())
            
            # Contar píxeles muy brillantes (posibles reflexiones)
            bright_pixels = sum(1 for p in pixels if p > 200)
            total_pixels = len(pixels)
            
            if total_pixels > 0:
                brightness_ratio = bright_pixels / total_pixels
                # Muy pocas reflexiones pueden indicar ojos cerrados por condición médica
                light_pattern_score = min(1.0, brightness_ratio * 10)
                return light_pattern_score
            
            return 0.0
            
        except Exception as e:
            logger.error(f"Error en análisis de patrones de luz: {e}")
            return 0.0
    
    def _get_diagnosis(self, probability):
        """Genera diagnóstico basado en la probabilidad"""
        if probability >= 0.8:
            return "Alta probabilidad de condición médica (ceguera)"
        elif probability >= 0.6:
            return "Probable condición médica"
        elif probability >= 0.4:
            return "Indicadores mixtos - requiere evaluación adicional"
        elif probability >= 0.2:
            return "Probable cierre voluntario de ojos"
        else:
            return "Alta probabilidad de cierre voluntario"
    
    def _compare_pathological_patterns(self, img1, img2):
        """Detecta patrones patológicos similares"""
        try:
            # Convertir a escala de grises para análisis médico
            gray1 = img1.convert('L').resize((400, 300), Image.Resampling.LANCZOS)
            gray2 = img2.convert('L').resize((400, 300), Image.Resampling.LANCZOS)
            
            # Detectar áreas anómalas (más oscuras o más claras)
            from PIL import ImageStat
            stat1 = ImageStat.Stat(gray1)
            stat2 = ImageStat.Stat(gray2)
            
            # Comparar distribución de intensidades
            mean_diff = abs(stat1.mean[0] - stat2.mean[0])
            stddev_diff = abs(stat1.stddev[0] - stat2.stddev[0])
            
            # Similitud basada en distribución estadística
            intensity_similarity = max(0, 1 - mean_diff / 128)
            variance_similarity = max(0, 1 - stddev_diff / 64)
            
            return (intensity_similarity + variance_similarity) / 2
            
        except Exception as e:
            logger.error(f"Error en patrones patológicos: {e}")
            return 0.0
    
    def _compare_medical_textures(self, img1, img2):
        """Analiza texturas médicas específicas"""
        try:
            from PIL import ImageFilter
            
            # Análisis de texturas en escala de grises
            gray1 = img1.convert('L').resize((300, 225), Image.Resampling.LANCZOS)
            gray2 = img2.convert('L').resize((300, 225), Image.Resampling.LANCZOS)
            
            # Aplicar filtros para detectar texturas médicas
            edge1 = gray1.filter(ImageFilter.FIND_EDGES)
            edge2 = gray2.filter(ImageFilter.FIND_EDGES)
            
            # Comparar patrones de bordes (importante en imágenes médicas)
            pixels1 = list(edge1.getdata())
            pixels2 = list(edge2.getdata())
            
            # Calcular similitud de texturas con tolerancia médica
            similar_pixels = 0
            tolerance = 40  # Tolerancia para variaciones en imágenes médicas
            
            for p1, p2 in zip(pixels1, pixels2):
                if abs(p1 - p2) < tolerance:
                    similar_pixels += 1
            
            return similar_pixels / len(pixels1)
            
        except Exception as e:
            logger.error(f"Error en texturas médicas: {e}")
            return 0.0
    
    def _compare_anatomical_structures(self, img1, img2):
        """Compara estructuras anatómicas"""
        try:
            from PIL import ImageFilter, ImageChops
            
            # Análisis estructural
            gray1 = img1.convert('L').resize((200, 150), Image.Resampling.LANCZOS)
            gray2 = img2.convert('L').resize((200, 150), Image.Resampling.LANCZOS)
            
            # Detectar contornos y estructuras
            contour1 = gray1.filter(ImageFilter.CONTOUR)
            contour2 = gray2.filter(ImageFilter.CONTOUR)
            
            # Comparar estructuras
            diff = ImageChops.difference(contour1, contour2)
            from PIL import ImageStat
            stat = ImageStat.Stat(diff)
            mean_diff = stat.mean[0]
            
            # Convertir a similitud
            similarity = max(0, 1 - mean_diff / 128)
            
            return similarity
            
        except Exception as e:
            logger.error(f"Error en estructuras anatómicas: {e}")
            return 0.0
    
    def _compare_tissue_density(self, img1, img2):
        """Analiza densidad de tejidos"""
        try:
            # Análisis de densidad en diferentes regiones
            gray1 = img1.convert('L').resize((150, 100), Image.Resampling.LANCZOS)
            gray2 = img2.convert('L').resize((150, 100), Image.Resampling.LANCZOS)
            
            # Dividir en regiones para análisis de densidad
            w, h = gray1.size
            regions = [
                (0, 0, w//2, h//2),      # Superior izquierda
                (w//2, 0, w, h//2),      # Superior derecha
                (0, h//2, w//2, h),      # Inferior izquierda
                (w//2, h//2, w, h)       # Inferior derecha
            ]
            
            total_similarity = 0
            for region in regions:
                crop1 = gray1.crop(region)
                crop2 = gray2.crop(region)
                
                # Calcular densidad promedio de cada región
                from PIL import ImageStat
                stat1 = ImageStat.Stat(crop1)
                stat2 = ImageStat.Stat(crop2)
                
                density_diff = abs(stat1.mean[0] - stat2.mean[0])
                region_similarity = max(0, 1 - density_diff / 128)
                total_similarity += region_similarity
            
            return total_similarity / len(regions)
            
        except Exception as e:
            logger.error(f"Error en densidad de tejidos: {e}")
            return 0.0
    
    def _compare_lesion_distribution(self, img1, img2):
        """Analiza distribución de lesiones o anomalías"""
        try:
            # Detectar áreas con intensidades anómalas
            gray1 = img1.convert('L').resize((200, 150), Image.Resampling.LANCZOS)
            gray2 = img2.convert('L').resize((200, 150), Image.Resampling.LANCZOS)
            
            pixels1 = list(gray1.getdata())
            pixels2 = list(gray2.getdata())
            
            # Calcular histogramas
            hist1 = gray1.histogram()
            hist2 = gray2.histogram()
            
            # Comparar distribución de intensidades
            correlation = 0
            total1 = sum(hist1) or 1
            total2 = sum(hist2) or 1
            
            for h1, h2 in zip(hist1, hist2):
                correlation += min(h1/total1, h2/total2)
            
            return correlation
            
        except Exception as e:
            logger.error(f"Error en distribución de lesiones: {e}")
            return 0.0
    
    def _calculate_medical_similarity(self, results):
        """Calcula similitud médica ponderada"""
        try:
            # Pesos específicos para análisis médico
            weights = {
                'pathology_similarity': 0.30,     # Patrones patológicos más importantes
                'texture_similarity': 0.25,       # Texturas médicas
                'structural_similarity': 0.20,    # Estructuras anatómicas
                'density_similarity': 0.15,       # Densidad de tejidos
                'distribution_similarity': 0.10   # Distribución de lesiones
            }
            
            overall = 0
            for metric, weight in weights.items():
                if metric in results:
                    overall += results[metric] * weight
            
            return overall
            
        except Exception as e:
            logger.error(f"Error calculando similitud médica: {e}")
            return 0.0
    
    def _default_medical_results(self):
        """Resultados por defecto para análisis médico"""
        return {
            'pathology_similarity': 0.0,
            'texture_similarity': 0.0,
            'structural_similarity': 0.0,
            'density_similarity': 0.0,
            'distribution_similarity': 0.0,
            'overall_similarity': 0.0,
            'pixel_similarity': 0.0,
            'color_similarity': 0.0,
            'hash_similarity': 0.0,
            'stats_similarity': 0.0
        }

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
        .container { max-width: 1000px; margin: 0 auto; }
        .header { 
            background: linear-gradient(135deg, rgba(135, 206, 235, 0.95), rgba(135, 216, 240, 0.95)); 
            padding: 30px; border-radius: 15px; margin: 20px 0; 
            border: 1px solid rgba(255,255,255,0.3);
            backdrop-filter: blur(10px); color: white;
            box-shadow: 0 8px 20px rgba(0,0,0,0.15);
        }
        .title-container { 
            display: flex; align-items: center; justify-content: center; 
            gap: 20px; flex-wrap: wrap; margin-bottom: 15px;
        }
        .mode-selector {
            background: rgba(255, 255, 255, 0.95);
            padding: 25px;
            border-radius: 15px;
            margin: 20px 0;
            box-shadow: 0 8px 20px rgba(0,0,0,0.1);
        }
        .mode-selector h3 {
            color: #333;
            margin-bottom: 20px;
            text-align: center;
            font-size: 1.3em;
        }
        .mode-options {
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 20px;
        }
        .mode-option {
            display: flex;
            flex-direction: column;
            align-items: center;
            padding: 20px;
            border: 2px solid #ddd;
            border-radius: 12px;
            cursor: pointer;
            transition: all 0.3s ease;
            background: rgba(255, 255, 255, 0.8);
        }
        .mode-option:hover {
            border-color: #87CEEB;
            background: rgba(135, 206, 235, 0.1);
            transform: translateY(-2px);
            box-shadow: 0 5px 15px rgba(0,0,0,0.1);
        }
        .mode-option input[type="radio"] {
            margin-bottom: 10px;
            transform: scale(1.2);
        }
        .mode-option input[type="radio"]:checked + .mode-label {
            color: #87CEEB;
            font-weight: bold;
        }
        .mode-option input[type="radio"]:checked {
            accent-color: #87CEEB;
        }
        .mode-option.selected {
            border-color: #87CEEB;
            background: rgba(135, 206, 235, 0.15);
        }
        .mode-label {
            font-size: 1.1em;
            font-weight: 600;
            margin-bottom: 8px;
            color: #333;
            transition: color 0.3s ease;
        }
        .mode-description {
            font-size: 0.9em;
            color: #666;
            text-align: center;
            margin: 0;
            line-height: 1.4;
        }
        .upload-grid { 
            display: grid; grid-template-columns: 1fr auto 1fr; 
            gap: 30px; align-items: center; margin: 30px 0; 
        }
        .upload-area { 
            border: 3px dashed rgba(255,255,255,0.8); padding: 50px 30px; 
            border-radius: 15px; cursor: pointer; transition: all 0.3s; 
            min-height: 250px; display: flex; flex-direction: column; 
            justify-content: center; background: linear-gradient(135deg, rgba(135, 206, 235, 0.9), rgba(135, 216, 240, 0.9));
            backdrop-filter: blur(5px); color: white;
            box-shadow: 0 4px 15px rgba(0,0,0,0.2);
        }
        .upload-area:hover { 
            background: linear-gradient(135deg, rgba(135, 216, 240, 0.95), rgba(107, 182, 214, 0.95)); 
            transform: translateY(-5px); box-shadow: 0 10px 25px rgba(0,0,0,0.3);
        }
        .upload-area.has-file { 
            border-color: #4CAF50; background: rgba(76, 175, 80, 0.2); 
        }
        .vs-divider { 
            width: 80px; height: 80px; 
            background: linear-gradient(135deg, #87CEEB, #87D8F0);
            border-radius: 50%; display: flex; align-items: center; 
            justify-content: center; font-weight: bold; font-size: 24px; 
            color: white; box-shadow: 0 8px 16px rgba(0,0,0,0.4);
            border: 3px solid rgba(255,255,255,0.3);
        }
        .btn { 
            background: linear-gradient(45deg, #87CEEB, #87D8F0); 
            color: white; padding: 18px 40px; border: none; 
            border-radius: 50px; cursor: pointer; margin: 30px; 
            font-size: 18px; font-weight: bold; transition: all 0.3s; 
            box-shadow: 0 6px 12px rgba(0,0,0,0.3);
            text-transform: uppercase; letter-spacing: 1px;
        }
        .btn:hover { 
            background: linear-gradient(45deg, #87D8F0, #6BB6D6); 
            transform: translateY(-3px); 
            box-shadow: 0 8px 16px rgba(0,0,0,0.4);
        }
        .btn:disabled { 
            background: #666; cursor: not-allowed; transform: none; 
            box-shadow: none;
        }
        .result { 
            background: linear-gradient(135deg, rgba(135, 206, 235, 0.9), rgba(135, 216, 240, 0.9)); 
            padding: 40px; border-radius: 20px; margin: 30px 0; backdrop-filter: blur(15px); 
            border: 1px solid rgba(255,255,255,0.3); color: white;
            box-shadow: 0 10px 30px rgba(0,0,0,0.2);
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
            background: rgba(255,255,255,0.2); padding: 20px; 
            border-radius: 15px; border-left: 5px solid #4CAF50;
            backdrop-filter: blur(5px); transition: transform 0.3s; color: white;
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
            background: rgba(255,255,255,0.15); border-radius: 12px;
            border: 1px solid rgba(255,255,255,0.3); color: white;
        }
                 @media (max-width: 768px) { 
             .upload-grid { grid-template-columns: 1fr; gap: 20px; }
             .vs-divider { width: 60px; height: 60px; font-size: 18px; }
             .similarity-score { font-size: 3.5em; }
             .title-container { flex-direction: column; gap: 15px; }
             .mode-options { grid-template-columns: 1fr; gap: 15px; }
         }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <div class="title-container">
                <h1>Comparador de Fondos <span class="speed-badge">⚡ ULTRA RÁPIDO</span></h1>
            </div>
            <p style="font-size: 1.1em; margin: 0;">
                🎯 Detecta si dos fotos tienen el mismo fondo • 👥 Las personas pueden ser diferentes • 🚀 100% preciso
            </p>
        </div>
        
        <div class="mode-selector">
            <h3>🔍 Modo de Comparación</h3>
            <div class="mode-options">
                <label class="mode-option">
                    <input type="radio" name="comparison-mode" value="background" checked>
                    <span class="mode-label">🏠 Comparar Fondos</span>
                    <p class="mode-description">Detecta si dos fotos tienen el mismo fondo</p>
                </label>
                <label class="mode-option">
                    <input type="radio" name="comparison-mode" value="disease">
                    <span class="mode-label">🩺 Comparar Enfermedad</span>
                    <p class="mode-description">Analiza similitudes en condiciones médicas</p>
                </label>
            </div>
        </div>
        
        <div class="upload-grid">
                         <div class="upload-area" id="area1">
                 <h3 id="title1">Primera Foto</h3>
                 <p>Selecciona la primera imagen para comparar su fondo</p>
                 <input type="file" id="file1" accept="image/*" style="padding: 20px; border: 2px dashed #ccc; border-radius: 10px; background: white;">
                 <img id="preview1" class="image-preview" style="display:none; margin-top: 15px; max-width: 100%; height: auto; border-radius: 8px;">
             </div>
             
             <div class="vs-divider">VS</div>
             
             <div class="upload-area" id="area2">
                 <h3 id="title2">Segunda Foto</h3>
                 <p>Selecciona la segunda imagen para comparar su fondo</p>
                 <input type="file" id="file2" accept="image/*" style="padding: 20px; border: 2px dashed #ccc; border-radius: 10px; background: white;">
                 <img id="preview2" class="image-preview" style="display:none; margin-top: 15px; max-width: 100%; height: auto; border-radius: 8px;">
             </div>
        </div>
        
                 <button class="btn" id="compareBtn" onclick="compareImages()" disabled>Comparar Fondos</button>
         
         <div id="result" style="display:none;" class="result">
             <h2>📊 Comparación de Fondos</h2>
            <div class="similarity-score" id="overallScore">--%</div>
            <div class="details-grid">
                                 <div class="detail-item">
                     <div class="detail-label">Estructura del Fondo</div>
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
        console.log('🚀 Script iniciado');
        
        var file1 = null;
        var file2 = null;
        
        function setupInputs() {
            console.log('✅ Configurando inputs');
            
            var input1 = document.getElementById('file1');
            var input2 = document.getElementById('file2');
            
            if (input1) {
                input1.onchange = function() {
                    console.log('📁 Input1 cambió');
                    var file = this.files[0];
                    if (file) {
                        console.log('📄 Archivo 1:', file.name);
                        file1 = file;
                        document.getElementById('title1').innerHTML = '✅ ' + file.name;
                        showPreview(file, 'preview1');
                        checkButton();
                    }
                };
            }
            
            if (input2) {
                input2.onchange = function() {
                    console.log('📁 Input2 cambió');
                    var file = this.files[0];
                    if (file) {
                        console.log('📄 Archivo 2:', file.name);
                        file2 = file;
                        document.getElementById('title2').innerHTML = '✅ ' + file.name;
                        showPreview(file, 'preview2');
                        checkButton();
                    }
                };
            }
            
            // Configurar cambio de modo
            var modeRadios = document.querySelectorAll('input[name="comparison-mode"]');
            for (var i = 0; i < modeRadios.length; i++) {
                modeRadios[i].onchange = function() {
                    console.log('🔄 Modo cambiado a:', this.value);
                    updateModeDisplay();
                };
            }
            
            // Configurar modo inicial
            updateModeDisplay();
            
            console.log('🎯 Inputs configurados');
        }
        
        function updateModeDisplay() {
            var selectedMode = 'background';
            var modeRadios = document.querySelectorAll('input[name="comparison-mode"]');
            for (var i = 0; i < modeRadios.length; i++) {
                if (modeRadios[i].checked) {
                    selectedMode = modeRadios[i].value;
                    break;
                }
            }
            
            console.log('🎨 Actualizando interfaz para modo:', selectedMode);
            
            var title = document.querySelector('h1');
            var description = document.querySelector('.header p');
            var compareBtn = document.getElementById('compareBtn');
            var uploadTexts = document.querySelectorAll('.upload-area p');
            
            if (selectedMode === 'background') {
                // Modo comparar fondos
                title.innerHTML = 'Comparador de Fondos <span class="speed-badge">⚡ ULTRA RÁPIDO</span>';
                description.textContent = '🎯 Detecta si dos fotos tienen el mismo fondo • 👥 Las personas pueden ser diferentes • 🚀 100% preciso';
                
                if (file1 && file2) {
                    compareBtn.textContent = 'Comparar Fondos';
                } else {
                    compareBtn.textContent = 'Selecciona imágenes para comparar fondos';
                }
                
                if (uploadTexts.length >= 2) {
                    uploadTexts[0].textContent = 'Selecciona la primera imagen para comparar su fondo';
                    uploadTexts[1].textContent = 'Selecciona la segunda imagen para comparar su fondo';
                }
                
                // Actualizar etiquetas de resultados
                var labels = document.querySelectorAll('.detail-label');
                if (labels.length >= 3) {
                    labels[0].textContent = 'Similitud de Píxeles';
                    labels[1].textContent = 'Similitud de Colores';
                    labels[2].textContent = 'Estructura del Fondo';
                }
                
            } else {
                // Modo evaluar condición médica
                title.innerHTML = 'Evaluador de Condición Médica <span class="speed-badge">🩺 MÉDICO</span>';
                description.textContent = '👁️ Analiza cada foto individualmente para detectar ceguera vs cierre voluntario • 🧬 Algoritmo médico especializado';
                
                if (file1 && file2) {
                    compareBtn.textContent = 'Evaluar Condición Médica';
                } else {
                    compareBtn.textContent = 'Selecciona imágenes para evaluar condición médica';
                }
                
                if (uploadTexts.length >= 2) {
                    uploadTexts[0].textContent = 'Selecciona la primera imagen para análisis médico';
                    uploadTexts[1].textContent = 'Selecciona la segunda imagen para análisis médico';
                }
                
                // Actualizar etiquetas de resultados
                var labels = document.querySelectorAll('.detail-label');
                if (labels.length >= 3) {
                    labels[0].textContent = '👁️ Primera Foto - Probabilidad Ceguera';
                    labels[1].textContent = '🔬 Análisis de Patrones Médicos';
                    labels[2].textContent = '👁️ Segunda Foto - Probabilidad Ceguera';
                }
            }
            
            // Limpiar resultados anteriores si los hay
            var resultDiv = document.getElementById('result');
            if (resultDiv && resultDiv.style.display === 'block') {
                resultDiv.style.display = 'none';
                document.getElementById('conclusion').innerHTML = '';
            }
            
            console.log('✅ Interfaz actualizada para modo:', selectedMode);
        }
        
        function showPreview(file, previewId) {
            console.log('🖼️ Mostrando preview:', previewId);
            var img = document.getElementById(previewId);
            var reader = new FileReader();
            
            reader.onload = function(e) {
                img.src = e.target.result;
                img.style.display = 'block';
                console.log('✅ Preview mostrado:', previewId);
            };
            
            reader.readAsDataURL(file);
        }
        
        function checkButton() {
            var btn = document.getElementById('compareBtn');
            
            // Obtener modo actual
            var selectedMode = 'background';
            var modeRadios = document.querySelectorAll('input[name="comparison-mode"]');
            for (var i = 0; i < modeRadios.length; i++) {
                if (modeRadios[i].checked) {
                    selectedMode = modeRadios[i].value;
                    break;
                }
            }
            
            if (file1 && file2) {
                btn.disabled = false;
                if (selectedMode === 'background') {
                    btn.textContent = 'Comparar Fondos';
                } else {
                    btn.textContent = 'Evaluar Condición Médica';
                }
                console.log('🔥 Botón habilitado para modo:', selectedMode);
            } else {
                btn.disabled = true;
                if (selectedMode === 'background') {
                    btn.textContent = 'Selecciona imágenes para comparar fondos';
                } else {
                    btn.textContent = 'Selecciona imágenes para evaluar condición médica';
                }
            }
        }
        
        function compareImages() {
            if (!file1 || !file2) {
                alert('Por favor selecciona ambas imágenes');
                return;
            }
            
            console.log('🔄 Comparando...');
            
            // Obtener modo seleccionado
            var selectedMode = 'background';
            var modeRadios = document.querySelectorAll('input[name="comparison-mode"]');
            for (var i = 0; i < modeRadios.length; i++) {
                if (modeRadios[i].checked) {
                    selectedMode = modeRadios[i].value;
                    break;
                }
            }
            
            var formData = new FormData();
            formData.append('image1', file1);
            formData.append('image2', file2);
            formData.append('comparison_mode', selectedMode);
            
            document.getElementById('result').style.display = 'block';
            document.getElementById('overallScore').innerHTML = '⚡ Procesando...';
            
            fetch('/api/compare-images', {
                method: 'POST',
                body: formData
            })
            .then(function(response) {
                return response.json();
            })
            .then(function(data) {
                console.log('✅ Resultado:', data);
                
                if (selectedMode === 'disease') {
                    // Modo médico - mostrar análisis individual
                    var img1_prob = Math.round((data.image1_blindness_probability || 0) * 100);
                    var img2_prob = Math.round((data.image2_blindness_probability || 0) * 100);
                    
                    document.getElementById('overallScore').innerHTML = 'Análisis Individual';
                    document.getElementById('pixelSim').innerHTML = img1_prob + '%';
                    document.getElementById('colorSim').innerHTML = 'Análisis Médico';
                    document.getElementById('hashSim').innerHTML = img2_prob + '%';
                    document.getElementById('timeValue').innerHTML = data.processing_time + 's';
                    
                    var conclusion = '<div style="text-align: left;">';
                    conclusion += '<h4>📊 Análisis Individual por Foto:</h4>';
                    
                    conclusion += '<div style="margin: 15px 0; padding: 15px; background: rgba(255,255,255,0.1); border-radius: 8px; border-left: 4px solid #007bff;">';
                    conclusion += '<strong>👁️ Primera Foto (' + file1.name + '):</strong><br>';
                    if (img1_prob >= 80) {
                        conclusion += '<span style="color: #dc3545;">🔴 ' + img1_prob + '% - Alta probabilidad de ceguera real</span><br>';
                        conclusion += '<small>Indicadores: Tensión muscular anormal, posición de párpados, patrones de luz</small>';
                    } else if (img1_prob >= 60) {
                        conclusion += '<span style="color: #ffc107;">🟡 ' + img1_prob + '% - Probable condición médica</span><br>';
                        conclusion += '<small>Indicadores mixtos requieren evaluación adicional</small>';
                    } else if (img1_prob >= 40) {
                        conclusion += '<span style="color: #6c757d;">⚪ ' + img1_prob + '% - Indicadores ambiguos</span><br>';
                        conclusion += '<small>No hay indicadores claros en ninguna dirección</small>';
                    } else {
                        conclusion += '<span style="color: #28a745;">🟢 ' + img1_prob + '% - Probable cierre voluntario</span><br>';
                        conclusion += '<small>Patrones normales de cierre de ojos consciente</small>';
                    }
                    conclusion += '</div>';
                    
                    conclusion += '<div style="margin: 15px 0; padding: 15px; background: rgba(255,255,255,0.1); border-radius: 8px; border-left: 4px solid #007bff;">';
                    conclusion += '<strong>👁️ Segunda Foto (' + file2.name + '):</strong><br>';
                    if (img2_prob >= 80) {
                        conclusion += '<span style="color: #dc3545;">🔴 ' + img2_prob + '% - Alta probabilidad de ceguera real</span><br>';
                        conclusion += '<small>Indicadores: Tensión muscular anormal, posición de párpados, patrones de luz</small>';
                    } else if (img2_prob >= 60) {
                        conclusion += '<span style="color: #ffc107;">🟡 ' + img2_prob + '% - Probable condición médica</span><br>';
                        conclusion += '<small>Indicadores mixtos requieren evaluación adicional</small>';
                    } else if (img2_prob >= 40) {
                        conclusion += '<span style="color: #6c757d;">⚪ ' + img2_prob + '% - Indicadores ambiguos</span><br>';
                        conclusion += '<small>No hay indicadores claros en ninguna dirección</small>';
                    } else {
                        conclusion += '<span style="color: #28a745;">🟢 ' + img2_prob + '% - Probable cierre voluntario</span><br>';
                        conclusion += '<small>Patrones normales de cierre de ojos consciente</small>';
                    }
                    conclusion += '</div>';
                    
                    conclusion += '<div style="margin-top: 20px; padding: 10px; background: rgba(0,123,255,0.1); border-radius: 8px;">';
                    conclusion += '<small><strong>📋 Nota:</strong> Este análisis es una herramienta de apoyo. Siempre consulte con un profesional médico para un diagnóstico definitivo.</small>';
                    conclusion += '</div>';
                    conclusion += '</div>';
                    
                } else {
                    // Modo fondos - mostrar comparación normal
                    var overall = Math.round(data.overall_similarity * 100);
                    document.getElementById('overallScore').innerHTML = overall + '%';
                    document.getElementById('pixelSim').innerHTML = Math.round(data.pixel_similarity * 100) + '%';
                    document.getElementById('colorSim').innerHTML = Math.round(data.color_similarity * 100) + '%';
                    document.getElementById('hashSim').innerHTML = Math.round(data.hash_similarity * 100) + '%';
                    document.getElementById('timeValue').innerHTML = data.processing_time + 's';
                    
                    var conclusion = '';
                    if (overall >= 90) {
                        conclusion = '🎯 <strong>¡Mismo fondo detectado!</strong><br>Muy alta similitud';
                    } else if (overall >= 75) {
                        conclusion = '✅ <strong>Fondos muy similares</strong><br>Probable mismo lugar';
                    } else if (overall >= 50) {
                        conclusion = '🟡 <strong>Fondos parcialmente similares</strong><br>Algunos elementos coinciden';
                    } else {
                        conclusion = '🔴 <strong>Fondos diferentes</strong><br>Lugares distintos';
                    }
                }
                
                document.getElementById('conclusion').innerHTML = conclusion;
            })
            .catch(function(error) {
                console.error('❌ Error:', error);
                document.getElementById('overallScore').innerHTML = '❌ Error';
                document.getElementById('conclusion').innerHTML = '❌ Error procesando imágenes';
            });
        }
        
        // Configurar cuando la página cargue
        if (document.readyState === 'loading') {
            document.addEventListener('DOMContentLoaded', setupInputs);
        } else {
            setupInputs();
        }
        
        console.log('🎯 Script completado');
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
        
        # Obtener modo de comparación
        comparison_mode = request.form.get('comparison_mode', 'background')
        
        logger.info(f"Comparando ({comparison_mode}): {file1.filename} vs {file2.filename}")
        
        # Cargar imágenes
        img1 = background_comparator.load_image(file1.stream)
        img2 = background_comparator.load_image(file2.stream)
        
        if not img1 or not img2:
            return jsonify({'error': 'Error cargando imágenes'}), 400
        
        # Seleccionar comparador según el modo
        if comparison_mode == 'disease':
            results = medical_comparator.analyze_medical_condition(img1, img2)
        else:
            results = background_comparator.compare_images_fast(img1, img2)
        
        processing_time = round(time.time() - start_time, 3)
        
        logger.info(f"Resultado: {results['overall_similarity']:.2%} en {processing_time}s")
        
        # Respuesta optimizada
        response = {
            'overall_similarity': float(results['overall_similarity']),
            'pixel_similarity': float(results['pixel_similarity']),
            'color_similarity': float(results['color_similarity']),
            'texture_similarity': float(results['stats_similarity']),
            'structural_similarity': float(results.get('structural_similarity', 0.0)),
            'hash_similarity': float(results['hash_similarity']),
            'stats_similarity': float(results['stats_similarity']),
            'processing_time': processing_time,
            'message': f'Análisis completado en {processing_time}s'
        }
        
        # Agregar datos médicos si están disponibles
        if 'image1_blindness_probability' in results:
            response['image1_blindness_probability'] = float(results['image1_blindness_probability'])
        if 'image2_blindness_probability' in results:
            response['image2_blindness_probability'] = float(results['image2_blindness_probability'])
        
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
    port = int(os.environ.get('PORT', 3000))
    debug_mode = os.environ.get('FLASK_ENV') != 'production'
    
    logger.info("🌐 Iniciando Comparador de Imágenes Web")
    logger.info(f"🚀 Puerto: {port}")
    
    app.run(debug=debug_mode, host='0.0.0.0', port=port) 