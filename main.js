/**
 * Comparador de Fondos de Imágenes - JavaScript
 * Maneja la interfaz de usuario y comunicación con el backend
 */

class BackgroundComparator {
    constructor() {
        this.images = {
            image1: null,
            image2: null
        };
        this.files = {
            file1: null,
            file2: null
        };
        
        this.initializeElements();
        this.attachEventListeners();
    }

    initializeElements() {
        // Upload areas
        this.uploadArea1 = document.getElementById('upload1');
        this.uploadArea2 = document.getElementById('upload2');
        
        // File inputs
        this.fileInput1 = document.getElementById('file1');
        this.fileInput2 = document.getElementById('file2');
        
        // Preview elements
        this.preview1 = document.getElementById('preview1');
        this.preview2 = document.getElementById('preview2');
        this.img1 = document.getElementById('img1');
        this.img2 = document.getElementById('img2');
        
        // Remove buttons
        this.removeBtn1 = document.getElementById('remove1');
        this.removeBtn2 = document.getElementById('remove2');
        
        // Compare button
        this.compareBtn = document.getElementById('compareBtn');
        
        // Results section
        this.resultsSection = document.getElementById('resultsSection');
        this.loadingOverlay = document.getElementById('loadingOverlay');
        
        // Result elements
        this.similarityScore = document.getElementById('similarityScore');
        this.colorSimilarity = document.getElementById('colorSimilarity');
        this.textureSimilarity = document.getElementById('textureSimilarity');
        this.structuralSimilarity = document.getElementById('structuralSimilarity');
        this.conclusionIcon = document.getElementById('conclusionIcon');
        this.conclusionText = document.getElementById('conclusionText');
        this.processedImg1 = document.getElementById('processedImg1');
        this.processedImg2 = document.getElementById('processedImg2');
    }

    attachEventListeners() {
        // File input events
        this.fileInput1.addEventListener('change', (e) => this.handleFileSelect(e, 1));
        this.fileInput2.addEventListener('change', (e) => this.handleFileSelect(e, 2));
        
        // Upload area events
        this.setupDragAndDrop(this.uploadArea1, this.fileInput1, 1);
        this.setupDragAndDrop(this.uploadArea2, this.fileInput2, 2);
        
        // Remove button events
        this.removeBtn1.addEventListener('click', () => this.removeImage(1));
        this.removeBtn2.addEventListener('click', () => this.removeImage(2));
        
        // Compare button
        this.compareBtn.addEventListener('click', () => this.compareImages());
        
        // Click to browse functionality
        this.uploadArea1.addEventListener('click', (e) => {
            if (e.target.classList.contains('browse-link') || 
                e.target.closest('.upload-content')) {
                this.fileInput1.click();
            }
        });
        
        this.uploadArea2.addEventListener('click', (e) => {
            if (e.target.classList.contains('browse-link') || 
                e.target.closest('.upload-content')) {
                this.fileInput2.click();
            }
        });
    }

    setupDragAndDrop(uploadArea, fileInput, imageNumber) {
        // Prevent default drag behaviors
        ['dragenter', 'dragover', 'dragleave', 'drop'].forEach(eventName => {
            uploadArea.addEventListener(eventName, this.preventDefaults, false);
            document.body.addEventListener(eventName, this.preventDefaults, false);
        });

        // Highlight drop area when item is dragged over it
        ['dragenter', 'dragover'].forEach(eventName => {
            uploadArea.addEventListener(eventName, () => {
                uploadArea.classList.add('dragover');
            }, false);
        });

        ['dragleave', 'drop'].forEach(eventName => {
            uploadArea.addEventListener(eventName, () => {
                uploadArea.classList.remove('dragover');
            }, false);
        });

        // Handle dropped files
        uploadArea.addEventListener('drop', (e) => {
            const files = e.dataTransfer.files;
            if (files.length > 0) {
                this.handleFile(files[0], imageNumber);
            }
        }, false);
    }

    preventDefaults(e) {
        e.preventDefault();
        e.stopPropagation();
    }

    handleFileSelect(event, imageNumber) {
        const file = event.target.files[0];
        if (file) {
            this.handleFile(file, imageNumber);
        }
    }

    handleFile(file, imageNumber) {
        // Validate file type
        if (!file.type.startsWith('image/')) {
            this.showNotification('Por favor, selecciona un archivo de imagen válido', 'error');
            return;
        }

        // Validate file size (max 10MB)
        const maxSize = 10 * 1024 * 1024; // 10MB
        if (file.size > maxSize) {
            this.showNotification('El archivo es demasiado grande. Máximo 10MB permitido', 'error');
            return;
        }

        // Store file reference
        this.files[`file${imageNumber}`] = file;

        // Create file reader
        const reader = new FileReader();
        
        reader.onload = (e) => {
            this.displayImage(e.target.result, imageNumber);
        };
        
        reader.readAsDataURL(file);
    }

    displayImage(imageSrc, imageNumber) {
        const uploadContent = imageNumber === 1 ? 
            this.uploadArea1.querySelector('.upload-content') : 
            this.uploadArea2.querySelector('.upload-content');
        
        const preview = imageNumber === 1 ? this.preview1 : this.preview2;
        const img = imageNumber === 1 ? this.img1 : this.img2;
        
        // Hide upload content and show preview
        uploadContent.style.display = 'none';
        preview.style.display = 'block';
        img.src = imageSrc;
        
        // Store image reference
        this.images[`image${imageNumber}`] = imageSrc;
        
        // Enable compare button if both images are loaded
        this.updateCompareButton();
        
        // Add animation
        preview.classList.add('fade-in');
        
        this.showNotification(`Imagen ${imageNumber} cargada exitosamente`, 'success');
    }

    removeImage(imageNumber) {
        const uploadContent = imageNumber === 1 ? 
            this.uploadArea1.querySelector('.upload-content') : 
            this.uploadArea2.querySelector('.upload-content');
        
        const preview = imageNumber === 1 ? this.preview1 : this.preview2;
        const img = imageNumber === 1 ? this.img1 : this.img2;
        const fileInput = imageNumber === 1 ? this.fileInput1 : this.fileInput2;
        
        // Reset elements
        uploadContent.style.display = 'flex';
        preview.style.display = 'none';
        img.src = '';
        fileInput.value = '';
        
        // Clear stored references
        this.images[`image${imageNumber}`] = null;
        this.files[`file${imageNumber}`] = null;
        
        // Update compare button
        this.updateCompareButton();
        
        // Hide results if visible
        this.hideResults();
    }

    updateCompareButton() {
        const bothImagesLoaded = this.images.image1 && this.images.image2;
        this.compareBtn.disabled = !bothImagesLoaded;
    }

    async compareImages() {
        if (!this.files.file1 || !this.files.file2) {
            this.showNotification('Por favor, carga ambas imágenes antes de comparar', 'warning');
            return;
        }

        this.showLoading();

        try {
            // Prepare form data
            const formData = new FormData();
            formData.append('image1', this.files.file1);
            formData.append('image2', this.files.file2);

            // Send request to backend
            const response = await fetch('/api/compare-backgrounds', {
                method: 'POST',
                body: formData
            });

            if (!response.ok) {
                throw new Error(`HTTP error! status: ${response.status}`);
            }

            const result = await response.json();
            
            this.hideLoading();
            this.displayResults(result);

        } catch (error) {
            console.error('Error comparing images:', error);
            this.hideLoading();
            
            // Show demo results for testing
            this.showNotification('Mostrando resultados de demostración (backend no disponible)', 'warning');
            this.displayDemoResults();
        }
    }

    displayResults(result) {
        // Update similarity score
        const overallSimilarity = Math.round(result.overall_similarity * 100);
        this.similarityScore.textContent = `${overallSimilarity}%`;
        
        // Update detailed metrics
        this.colorSimilarity.textContent = `${Math.round(result.color_similarity * 100)}%`;
        this.textureSimilarity.textContent = `${Math.round(result.texture_similarity * 100)}%`;
        this.structuralSimilarity.textContent = `${Math.round(result.structural_similarity * 100)}%`;
        
        // Update conclusion
        this.updateConclusion(overallSimilarity);
        
        // Update result card styling
        this.updateResultCardStyling(overallSimilarity);
        
        // Display processed images
        if (result.processed_image1) {
            this.displayProcessedImage(result.processed_image1, 1);
        }
        if (result.processed_image2) {
            this.displayProcessedImage(result.processed_image2, 2);
        }
        
        // Show results section
        this.showResults();
    }

    displayDemoResults() {
        // Generate random but realistic demo results
        const overallSimilarity = Math.floor(Math.random() * 40) + 30; // 30-70%
        const colorSimilarity = Math.floor(Math.random() * 30) + 40; // 40-70%
        const textureSimilarity = Math.floor(Math.random() * 35) + 25; // 25-60%
        const structuralSimilarity = Math.floor(Math.random() * 45) + 20; // 20-65%
        
        this.similarityScore.textContent = `${overallSimilarity}%`;
        this.colorSimilarity.textContent = `${colorSimilarity}%`;
        this.textureSimilarity.textContent = `${textureSimilarity}%`;
        this.structuralSimilarity.textContent = `${structuralSimilarity}%`;
        
        this.updateConclusion(overallSimilarity);
        this.updateResultCardStyling(overallSimilarity);
        
        // Show placeholder for processed images
        this.showProcessedImagePlaceholder(1);
        this.showProcessedImagePlaceholder(2);
        
        this.showResults();
    }

    updateConclusion(similarity) {
        let iconClass, conclusion;
        
        if (similarity >= 70) {
            iconClass = 'success';
            conclusion = '¡Los fondos son muy similares! Es muy probable que sean el mismo fondo o muy parecidos.';
        } else if (similarity >= 50) {
            iconClass = 'warning';
            conclusion = 'Los fondos tienen similitudes moderadas. Podrían ser el mismo fondo con diferentes condiciones de iluminación o ángulo.';
        } else if (similarity >= 30) {
            iconClass = 'neutral';
            conclusion = 'Los fondos tienen algunas similitudes básicas, pero son diferentes en su mayoría.';
        } else {
            iconClass = 'error';
            conclusion = 'Los fondos son significativamente diferentes. No parecen ser el mismo lugar.';
        }
        
        // Update icon
        this.conclusionIcon.className = `conclusion-icon ${iconClass}`;
        const iconElement = this.conclusionIcon.querySelector('i');
        
        switch (iconClass) {
            case 'success':
                iconElement.className = 'fas fa-check';
                break;
            case 'warning':
                iconElement.className = 'fas fa-exclamation-triangle';
                break;
            case 'error':
                iconElement.className = 'fas fa-times';
                break;
            default:
                iconElement.className = 'fas fa-minus';
        }
        
        // Update text
        this.conclusionText.textContent = conclusion;
    }

    updateResultCardStyling(similarity) {
        const resultCard = document.querySelector('.result-card');
        
        // Remove existing classes
        resultCard.classList.remove('high-similarity', 'medium-similarity', 'low-similarity');
        
        // Add appropriate class
        if (similarity >= 70) {
            resultCard.classList.add('high-similarity');
        } else if (similarity >= 40) {
            resultCard.classList.add('medium-similarity');
        } else {
            resultCard.classList.add('low-similarity');
        }
    }

    displayProcessedImage(imageData, imageNumber) {
        const container = imageNumber === 1 ? this.processedImg1 : this.processedImg2;
        container.innerHTML = `<img src="data:image/jpeg;base64,${imageData}" alt="Fondo procesado ${imageNumber}">`;
    }

    showProcessedImagePlaceholder(imageNumber) {
        const container = imageNumber === 1 ? this.processedImg1 : this.processedImg2;
        container.innerHTML = `
            <div class="demo-placeholder">
                <i class="fas fa-image"></i>
                <p>Imagen procesada ${imageNumber}<br><small>(Demo - Backend requerido)</small></p>
            </div>
        `;
    }

    showResults() {
        this.resultsSection.style.display = 'block';
        this.resultsSection.classList.add('slide-up');
        
        // Smooth scroll to results
        setTimeout(() => {
            this.resultsSection.scrollIntoView({ 
                behavior: 'smooth', 
                block: 'start' 
            });
        }, 300);
    }

    hideResults() {
        this.resultsSection.style.display = 'none';
        this.resultsSection.classList.remove('slide-up');
    }

    showLoading() {
        this.loadingOverlay.style.display = 'flex';
        document.body.style.overflow = 'hidden';
    }

    hideLoading() {
        this.loadingOverlay.style.display = 'none';
        document.body.style.overflow = 'auto';
    }

    showNotification(message, type = 'info') {
        // Create notification element
        const notification = document.createElement('div');
        notification.className = `notification notification-${type}`;
        notification.innerHTML = `
            <i class="fas fa-${this.getNotificationIcon(type)}"></i>
            <span>${message}</span>
        `;
        
        // Style the notification
        Object.assign(notification.style, {
            position: 'fixed',
            top: '20px',
            right: '20px',
            padding: '15px 20px',
            borderRadius: '8px',
            color: 'white',
            fontSize: '14px',
            fontWeight: '500',
            zIndex: '10000',
            display: 'flex',
            alignItems: 'center',
            gap: '10px',
            minWidth: '300px',
            boxShadow: '0 4px 12px rgba(0,0,0,0.15)',
            transform: 'translateX(100%)',
            transition: 'transform 0.3s ease'
        });
        
        // Set background color based on type
        const colors = {
            success: '#48bb78',
            error: '#f56565',
            warning: '#ed8936',
            info: '#667eea'
        };
        notification.style.background = colors[type] || colors.info;
        
        // Add to document
        document.body.appendChild(notification);
        
        // Animate in
        setTimeout(() => {
            notification.style.transform = 'translateX(0)';
        }, 100);
        
        // Auto remove after 4 seconds
        setTimeout(() => {
            notification.style.transform = 'translateX(100%)';
            setTimeout(() => {
                if (notification.parentNode) {
                    notification.parentNode.removeChild(notification);
                }
            }, 300);
        }, 4000);
    }

    getNotificationIcon(type) {
        const icons = {
            success: 'check-circle',
            error: 'exclamation-circle',
            warning: 'exclamation-triangle',
            info: 'info-circle'
        };
        return icons[type] || icons.info;
    }
}

// Initialize the application when DOM is loaded
document.addEventListener('DOMContentLoaded', () => {
    window.backgroundComparator = new BackgroundComparator();
    
    // Add some CSS for notifications dynamically
    const style = document.createElement('style');
    style.textContent = `
        .demo-placeholder {
            text-align: center;
            color: var(--text-secondary);
            padding: 40px 20px;
        }
        .demo-placeholder i {
            font-size: 3rem;
            margin-bottom: 15px;
            display: block;
            opacity: 0.5;
        }
        .demo-placeholder p {
            margin: 0;
            line-height: 1.4;
        }
        .demo-placeholder small {
            opacity: 0.7;
            font-size: 0.8em;
        }
    `;
    document.head.appendChild(style);
}); 