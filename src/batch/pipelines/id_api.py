import cv2
import numpy as np
import random

def generate_synthetic_id():
    width, height = 800, 500
    id_card = np.ones((height, width, 3), dtype=np.uint8) * 255  # White background
    
    # Draw ID card outline
    cv2.rectangle(id_card, (10, 10), (width-10, height-10), (0, 0, 0), 3)
    
    # Generate random ID details
    name = "John Doe"
    dob = "1990-05-15"
    id_number = f"{random.randint(100000, 999999)}-{random.randint(1000, 9999)}"
    
    # Add text details
    font = cv2.FONT_HERSHEY_SIMPLEX
    cv2.putText(id_card, "Synthetic ID Card", (250, 50), font, 1, (0, 0, 0), 2)
    cv2.putText(id_card, f"Name: {name}", (50, 150), font, 0.8, (0, 0, 0), 2)
    cv2.putText(id_card, f"DOB: {dob}", (50, 200), font, 0.8, (0, 0, 0), 2)
    cv2.putText(id_card, f"ID No: {id_number}", (50, 250), font, 0.8, (0, 0, 0), 2)
    
    # Generate and place synthetic face
    face = np.ones((150, 150, 3), dtype=np.uint8) * 200  # Placeholder for face image
    cv2.rectangle(face, (10, 10), (140, 140), (100, 100, 100), -1)  # Face box
    id_card[100:250, 600:750] = face  # Place face on ID card
    
    # Save and display
    cv2.imwrite("synthetic_id_card.jpg", id_card)
    cv2.imshow("Synthetic ID Card", id_card)
    cv2.waitKey(0)
    cv2.destroyAllWindows()

# Generate synthetic ID card
generate_synthetic_id()
