import torch
from diffusers import StableDiffusionPipeline

def generate_realistic_face():
    model_id = "runwayml/stable-diffusion-v1-5"  # Pre-trained Stable Diffusion model
    pipe = StableDiffusionPipeline.from_pretrained(model_id, torch_dtype=torch.float16)
    pipe.to("cuda" if torch.cuda.is_available() else "cpu")
    
    prompt = "A highly detailed, photorealistic portrait of a person, studio lighting, ultra high definition"
    image = pipe(prompt).images[0]
    
    image.save("realistic_face.png")
    image.show()
    print("Generated face saved as realistic_face.png")

# Generate AI-generated face
generate_realistic_face()
